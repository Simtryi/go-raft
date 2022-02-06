package raft

import (
	"raft/rpc"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

type Raft struct {
	mu 					sync.Mutex
	id      			int             	//	current server's index of peers
	peers				[]*rpc.Host 		//	all raft servers
	persister 			*Persister

	voteCh				chan struct{}		//	grant vote channel
	entryCh				chan struct{}		//	append entry channel
	leaderCh			chan struct{}
	commitCh			chan struct{}
	applyCh				chan ApplyMsg
	exitCh				chan struct{}

	/* ----- state a raft server must maintain ----- */
	state 				State				//	current server state
	heartBeat 			time.Duration		//	heart beat interval

	/* ----- persistent state on all servers ----- */
	currentTerm 		int					//	latest term server has seen(initialized to 0 on first boot, increases monotonically)
	votedFor    		int  				//	candidateId that received vote in current term(or null if none)
	log 				[]Entry				//	log entries, each entry contains command for state machine, and term when entry was received by leader(first index is 1, 0 is an empty log)
	lastIncludedIndex 	int
	lastIncludedTerm  	int

	/* ----- volatile state on all servers ----- */
	commitIndex			int					//	index of the highest log entry known to be committed
	lastApplied			int					//	index of the highest log entry applied to state machine(initialized to 0, increases monotonically)

	/* ----- volatile state on leaders ----- */
	nextIndex			[]int				//	for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex			[]int				//	for each server, index of the highest log entry known to be replicated on server(initialized to 0, increases monotonically)
}

const HeartBeatInterval = 50

//	create raft server
func Make(id int, peers []*rpc.Host, persister *Persister, applyCh chan ApplyMsg) *Raft {
	r := new(Raft)
	r.id = id
	r.peers = peers
	r.persister = persister

	r.voteCh = make(chan struct{}, 1)
	r.entryCh = make(chan struct{}, 1)
	r.leaderCh = make(chan struct{}, 1)
	r.commitCh = make(chan struct{}, 1)
	r.applyCh = applyCh
	r.exitCh = make(chan struct{}, 1)

	r.state = Follower
	r.heartBeat = time.Duration(HeartBeatInterval) * time.Millisecond

	r.currentTerm = 0
	r.votedFor = VOTENULL
	r.log = make([]Entry, 1)
	r.lastIncludedIndex = 0
	r.lastIncludedTerm = -1

	r.commitIndex = 0
	r.lastApplied = 0

	//	initialize from state and snapshot persisted before a crash
	r.read()
	r.log[0] = Entry{
		Index: r.lastIncludedIndex,
		Term:  r.lastIncludedTerm,
	}
	r.lastApplied = r.lastIncludedIndex
	r.commitIndex = r.lastIncludedIndex
	DPrintf("[Raft] make server(%d)\n", r.id)

	go r.run()
	go r.applyLogs()

	return r
}

//	run raft server
func (r *Raft) run() {
	Loop:
	for {
		select {
		case <- r.exitCh:
			DPrintf("[Raft] exit server(%d)\n", r.id)
			break Loop
		default:
		}

		//	raft uses randomized election timeouts to ensure that split votes are rare and that they are resolved quickly,
		//	election timeouts are chosen randomly from a fixed interval
		electionTimeout := RandomTime()

		r.mu.Lock()
		state := r.state
		r.mu.Unlock()

		switch state {
		case Follower:
			select {
			case <- r.voteCh:
			case <- r.entryCh:
			case <- time.After(electionTimeout):
				r.mu.Lock()
				//	if election timeout elapses without receiving AppendEntries rpc from current leader
				//	or granting vote to candidate, convert to candidate
				r.convertToCandidate()
				r.mu.Unlock()
			}

		case Candidate:
			go r.leaderElection()
			select {
			case <- r.voteCh:
			case <- r.entryCh:
			case <- r.leaderCh:
			case <- time.After(electionTimeout):
				r.mu.Lock()
				//	if election timeout elapses, start new election
				r.convertToCandidate()
				r.mu.Unlock()
			}

		case Leader:
			//	upon election, send initial empty AppendEntries RPCs(heart beat) to each server;
			//	repeat during idle periods to prevent election timeouts
			r.logReplication()
			time.Sleep(r.heartBeat)
		}
	}
}



func (r *Raft) leaderElection() {
	r.mu.Lock()
	args := RequestVoteArgs{
		Term:         r.currentTerm,
		CandidateId:  r.id,
		LastLogIndex: r.getLastLogIndex(),
		LastLogTerm:  r.getLastLogTerm(),
	}
	r.mu.Unlock()

	//	number of votes received by the current candidate
	//	(initialized to 1, candidate vote for itself)
	var numVoted int32 = 1

	//	issues RequestVote RPCs in parallel to each of the other servers in the cluster
	for i := 0; i < len(r.peers); i++ {
		if i == r.id {
			continue
		}

		r.mu.Lock()
		//	only candidate can election
		if r.state != Candidate {
			r.mu.Unlock()
			return
		}
		r.mu.Unlock()

		go func(id int) {
			DPrintf("[LeaderElection] server(%d) send RequestVote RPC to server(%d), args:%v\n",
				r.id, id, args)

			reply := new(RequestVoteReply)
			if ok := r.sendRequestVote(id, args, reply); ok {
				r.mu.Lock()
				defer r.mu.Unlock()

				//	server’s term > candidate's term, candidate convert to follower
				if reply.Term > r.currentTerm {
					r.convertToFollower(reply.Term)
					return
				}

				//	determine whether the current candidate is still a candidate
				if !(r.state == Candidate && r.currentTerm == args.Term) {
					return
				}

				if reply.VoteGranted {
					atomic.AddInt32(&numVoted, 1)
				}

				//	a candidate wins an election
				//	if it receives votes from a majority of the servers
				//	in the full cluster for the same term
				if atomic.LoadInt32(&numVoted) > int32(len(r.peers) / 2) {
					DropAndSet(r.leaderCh)
					r.convertToLeader()

					DPrintf("[LeaderElection] server(%d) win election\n", r.id)
				}
			}
		}(i)
	}
}

func (r *Raft) logReplication() {
	//	issues AppendEntries RPCs in parallel to each of the other servers in the cluster
	for i := 0; i < len(r.peers); i++ {
		if i == r.id {
			continue
		}

		go func(id int) {
			//	if followers crash or run slowly, or if network packets are lost,
			//	the leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
			//	until all followers eventually store all log entries.
			for {
				r.mu.Lock()
				//	only leader can log replication
				if r.state != Leader {
					r.mu.Unlock()
					return
				}

				nextIndex := r.nextIndex[id]
				if nextIndex <= r.getLastIncludedIndex() {
					r.logCompaction(id)
					return
				}

				entries := make([]Entry, 0)
				entries = append(entries, r.log[nextIndex - r.getLastIncludedIndex():]...)

				args := AppendEntriesArgs{
					Term:         r.currentTerm,
					LeaderId:     r.id,
					PrevLogIndex: r.getPrevLogIndex(id),
					PrevLogTerm:  r.getPrevLogTerm(id),
					Entries: 	  entries,
					LeaderCommit: r.commitIndex,
				}
				r.mu.Unlock()

				DPrintf("[LogReplication] server(%d) send LogReplication RPC to server(%d), args:%v\n",
					r.id, id, args)

				reply := new(AppendEntriesReply)
				if ok := r.sendAppendEntries(id, args, reply); !ok {
					return
				}

				r.mu.Lock()

				//	server’s term > leader's term, leader convert to follower
				if reply.Term > r.currentTerm {
					r.convertToFollower(reply.Term)
					r.mu.Unlock()
					return
				}

				//	determine whether the current leader is still a leader
				if !(r.state == Leader && r.currentTerm == args.Term) {
					r.mu.Unlock()
					return
				}

				if reply.Success {	//	append entries success
					//	update follower's nextIndex and matchIndex
					r.matchIndex[id] = args.PrevLogIndex + len(args.Entries)
					r.nextIndex[id] = r.matchIndex[id] + 1

					//	update leader's commitIndex
					r.updateCommitIndex()

					r.mu.Unlock()
					DPrintf("[LogReplication] server(%d) append entries to server(%d) success\n", r.id, id)
					return
				} else {	//	append entries fail
					if reply.ConflictTerm == -1 {
						//	follower lack logs or already snapshot
						r.nextIndex[id] = reply.ConflictIndex
					} else {
						//	term conflict, leader decrement nextIndex to bypass all the conflicting entries in that term
						for j := r.getLastIncludedIndex() + 1; j < len(r.log); j++ {
							if r.log[j - r.getLastIncludedIndex()].Term == reply.ConflictTerm {
								reply.ConflictIndex = j + 1
							}
						}
						r.nextIndex[id] = Max(r.getLastIncludedIndex() + 1, reply.ConflictIndex)
					}

					r.mu.Unlock()
					DPrintf("[LogReplication] server(%d) append entries to server(%d) failed\n", r.id, id)
				}
			}
		}(i)
	}
}

func (r *Raft) logCompaction(id int) {
	args := InstallSnapshotArgs{
		Term: 			   r.currentTerm,
		LeaderId: 		   r.id,
		LastIncludedIndex: r.lastIncludedIndex,
		LastIncludedTerm:  r.lastIncludedTerm,
		Data: 			   r.persister.ReadSnapshot(),
	}
	r.mu.Unlock()

	DPrintf("[LogCompaction] server(%d) send InstallSnapshot RPC to server(%d), args: %v\n",
		r.id, id, args)

	reply := new(InstallSnapshotReply)
	if ok := r.sendInstallSnapshot(id, args, reply); !ok {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	//	server’s term > leader's term, leader convert to follower
	if reply.Term > r.currentTerm {
		r.convertToFollower(reply.Term)
		return
	}

	//	determine whether the current leader is still a leader
	if !(r.state == Leader && r.currentTerm == args.Term) {
		return
	}

	//	update follower's nextIndex and matchIndex
	r.nextIndex[id] = args.LastIncludedIndex + 1
	r.matchIndex[id] = args.LastIncludedIndex
	DPrintf("[LogCompaction] server(%d) install snapshot to server(%d) success\n", r.id, id)
}

//	if there exists an N such that N > commitIndex,
//	a majority of matchIndex[i] ≥ N,
//	and log[N].term == currentTerm: set commitIndex = N
func (r *Raft) updateCommitIndex() {
	matchIndexes := make([]int, len(r.matchIndex))
	copy(matchIndexes, r.matchIndex)
	matchIndexes[r.id] = r.lastIncludedIndex + len(r.log) - 1
	sort.Ints(matchIndexes)

	N := matchIndexes[len(r.peers) / 2]
	if r.state == Leader && N > r.commitIndex && r.log[N - r.lastIncludedIndex].Term == r.currentTerm {
		r.commitIndex = N
		DropAndSet(r.commitCh)
	}
}



func (r *Raft) Start(command interface{}) (int, int, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	index := -1
	term := r.currentTerm
	isLeader := r.state == Leader

	if isLeader {
		index = r.getLastLogIndex() + 1
		entry := Entry{
			Index:   index,
			Term:    term,
			Command: command,
		}

		r.log = append(r.log, entry)
		r.save()
		DPrintf("[Raft] server(%d) append entry(index: %d, term: %d, command: %v)\n", r.id, index, term, command)
	}

	return index, term, isLeader
}

// 	the service says it has created a snapshot that has all info up to and including index,
//	this means the service no longer needs the log through (and including) that index,
//	Raft should now trim its log as much as possible
func (r *Raft) Snapshot(index int, snapshot []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()

	//	ignore old snapshot
	if index <= r.getLastIncludedIndex() {
		return
	}

	newLog := make([]Entry, 0)
	newLog = append(newLog, r.log[index - r.getLastIncludedIndex():]...)

	r.lastIncludedTerm = r.log[index - r.lastIncludedIndex].Term
	r.lastIncludedIndex = index
	r.log = newLog

	r.commitIndex = Max(r.lastIncludedIndex, r.commitIndex)
	r.lastApplied = Max(r.lastIncludedIndex, r.lastApplied)

	state := r.encodeState()
	r.persister.SaveStateAndSnapshot(state, snapshot)
	DPrintf("[Raft] server(%d) create snapshot(index: %d, term: %d)\n", r.id, r.lastIncludedIndex, r.lastIncludedTerm)

	return
}

//	a service wants to switch to snapshot,
//	only do so if Raft hasn't had more recent info since it communicate the snapshot on applyCh
func (r *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	r.mu.Lock()
	defer r.mu.Unlock()

	if lastIncludedIndex <= r.commitIndex {
		return false
	}

	newLog := make([]Entry, 0)
	if lastIncludedIndex <= r.getLastLogIndex() && lastIncludedTerm == r.getLastIncludedTerm() {
		//	if existing log entry has same index and term as snapshot’s last included entry,
		//	retain log entries following it and reply
		for i := lastIncludedIndex - r.getLastIncludedIndex(); i < len(r.log); i++ {
			newLog = append(newLog, r.log[i])
		}
	} else {
		newLog = append(newLog, Entry{Index: lastIncludedIndex, Term: lastIncludedTerm})
	}
	r.log = newLog

	r.lastIncludedIndex = lastIncludedIndex
	r.lastIncludedTerm = lastIncludedTerm

	r.commitIndex = lastIncludedIndex
	r.lastApplied = lastIncludedIndex

	state := r.encodeState()
	r.persister.SaveStateAndSnapshot(state, snapshot)
	DPrintf("[Raft] server(%d) install snapshot(index: %d, term: %d) success\n", r.id, r.lastIncludedIndex, r.lastIncludedTerm)

	return true
}

//	return currentTerm and whether this server
//	believes it is the leader
func (r *Raft) GetState() (currentTerm int, isLeader bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	currentTerm = r.currentTerm
	isLeader = r.state == Leader
	return
}

//	exit raft server
func (r *Raft) Exit() {
	DropAndSet(r.exitCh)
}