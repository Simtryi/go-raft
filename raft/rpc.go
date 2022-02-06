package raft

import (
	"strconv"
)

/* -------------------- leader election: begin -------------------- */

type RequestVoteArgs struct {
	Term 			int		//	candidate's term
	CandidateId 	int 	//	candidate id
	LastLogIndex	int		//	index of candidate's last log entry
	LastLogTerm 	int		//	index of candidate's last log entry
}

type RequestVoteReply struct {
	Term 			int 	//	currentTerm, for candidate to update itself
	VoteGranted 	bool	//	true mean candidate received vote
}

//	invoked by candidates to gather votes
func (r *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.save()

	//	args.term < r.currentTerm, return r.currentTerm
	//	args.term > r.currentTerm, return args.term
	reply.Term = r.currentTerm
	reply.VoteGranted = false

	//	candidate's term < current server's term, refuse to vote
	if args.Term < r.currentTerm {
		return
	}

	//	candidate's term > current server's term, convert to follower
	if args.Term > r.currentTerm {
		reply.Term = args.Term
		r.convertToFollower(args.Term)
	}

	if r.voteCheck(args) {
		DropAndSet(r.voteCh)
		r.state = Follower
		r.votedFor = args.CandidateId

		//	meet the conditions, agree to vote
		reply.VoteGranted = true

		DPrintf("[LeaderElection] server(id: %d, term: %d) vote for server(id: %d, term: %d)\n",
			r.id, r.currentTerm, args.CandidateId, args.Term)
	}
}

//	if votedFor is null or candidateId,
//	and candidate's log is at least as up-to-date as receiver's log, grant vote
func (r *Raft) voteCheck(args RequestVoteArgs) bool {
	//	current server has not voted, or voted for the current candidate
	if (r.votedFor == VOTENULL || r.votedFor == args.CandidateId) &&
		args.LastLogTerm > r.getLastLogTerm() ||
		(args.LastLogTerm == r.getLastLogTerm() &&
			args.LastLogIndex >= r.getLastLogIndex()) {
		return true
	}

	return false
}

//	send RequestVote RPC
func (r *Raft) sendRequestVote(serverId int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	serverAddress := strconv.Itoa(serverId)
	return r.peers[r.id].Call(serverAddress, "Raft.RequestVote", args, reply)
}

/* -------------------- leader election: end -------------------- */



/* -------------------- log replication: begin -------------------- */

type AppendEntriesArgs struct {
	Term 			int			//	leader's term
	LeaderId		int			//	leader id
	PrevLogIndex	int			//	index of log entry immediately preceding new ones
	PrevLogTerm		int			//	term of PrevLogIndex entry
	Entries  		[]Entry		//	log entries to store(empty for heartbeat; may send more than one for efficiency)
	LeaderCommit 	int 		//	leader's commitIndex
}

type AppendEntriesReply struct {
	Term 			int 		//	currentTerm, for leader to update itself
	Success			bool		//	true if follower contained entry matching PrevLogIndex and PrevLogTerm
	ConflictIndex	int			//	index of conflict log entry
	ConflictTerm	int			//	term of conflict log entry
}

//	invoked by leader to replicate log Entries; also used as heartbeat
func (r *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.save()

	//	args.Term < r.currentTerm, return r.currentTerm
	//	args.Term > r.currentTerm, return args.Term
	reply.Term = r.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	//	leader's term < current server's term, refuse to append entries
	if args.Term < r.currentTerm {
		return
	}

	//	leader's term > current server's term, convert to follower
	if args.Term > r.currentTerm {
		reply.Term = args.Term
		r.convertToFollower(args.Term)
	}

	DropAndSet(r.entryCh)
	r.state = Follower

	if r.consistencyCheck(args, reply) {
		reply.Success = true

		for i := 0; i < len(args.Entries); i++{
			index := args.PrevLogIndex + 1 - r.getLastIncludedIndex() + i

			if index < len(r.log) && r.log[index].Term != args.Entries[i].Term {
				//	follower's log conflict, delete conflict log
				r.log = r.log[:index]
				r.log = append(r.log, args.Entries[i:]...)
				break
			} else if index >= len(r.log) {
				//	no conflict, direct replication
				r.log = append(r.log, args.Entries[i:]...)
				break
			}
		}

		//	LeaderCommit > commitIndex, set commitIndex = min(LeaderCommit, index of last new entry)
		if args.LeaderCommit > r.commitIndex {
			r.commitIndex = Min(args.LeaderCommit, r.getLastLogIndex())
			DropAndSet(r.commitCh)
		}
		DPrintf("[LogReplication] server(%d) append entries success\n", r.id)
	}
}

func (r *Raft) consistencyCheck(args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//	follower lack logs
	if args.PrevLogIndex > r.getLastLogIndex() {
		reply.ConflictIndex = r.getLastLogIndex() + 1
		reply.ConflictTerm = -1
		return false
	}

	//	already snapshot
	if args.PrevLogIndex < r.getLastIncludedIndex() {
		reply.ConflictIndex = r.getLastIncludedIndex() + 1
		reply.ConflictTerm = -1
		return false
	}

	//	if the follower does not find an entry in its log with the same index and term,
	//	then it refuses the new Entries
	if args.PrevLogTerm != r.log[args.PrevLogIndex - r.getLastIncludedIndex()].Term {
		reply.ConflictTerm = r.log[args.PrevLogIndex - r.getLastIncludedIndex()].Term

		//	when rejecting an AppendEntries request,
		//	the follower can include the term of the conflicting entry
		//	and the first index it stores for that term
		for i := r.getLastIncludedIndex() + 1; i <= args.PrevLogIndex; i++ {
			if r.log[i - r.getLastIncludedIndex()].Term == reply.ConflictTerm {
				reply.ConflictIndex = i
				break
			}
		}
		return false
	}

	return true
}

//	send AppendEntries RPC
func (r *Raft) sendAppendEntries(serverId int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	serverAddress := strconv.Itoa(serverId)
	return r.peers[r.id].Call(serverAddress, "Raft.AppendEntries", args, reply)
}

/* -------------------- log replication: end -------------------- */



/* -------------------- install snapshot: begin -------------------- */

type InstallSnapshotArgs struct {
	Term 				int		//	leader’s term
	LeaderId 			int		//	so follower can redirect clients
	LastIncludedIndex 	int		//	the snapshot replaces all entries up through and including this index
	LastIncludedTerm 	int		//	term of lastIncludedIndex
	Data 				[]byte	//	raw bytes of the snapshot chunk, starting at offset
}

type InstallSnapshotReply struct {
	Term  				int 	//	currentTerm, for leader to update itself
}

//	invoked by leader to send chunks of a snapshot to a follower
func (r *Raft) InstallSnapshot(args InstallSnapshotArgs, reply *InstallSnapshotReply) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer r.save()

	//	args.term > r.currentTerm, return args.term
	//	args.term < r.currentTerm, return r.currentTerm
	reply.Term = r.currentTerm

	//	leader's term/lastIncludedIndex < current server's term/lastIncludedIndex, refuse to install snapshot
	if args.Term < r.currentTerm || args.LastIncludedIndex <= r.getLastIncludedIndex() {
		return
	}

	//	leader's term > current server's term, convert to follower
	if args.Term > r.currentTerm {
		reply.Term = args.Term
		r.convertToFollower(args.Term)
	}

	DropAndSet(r.entryCh)
	r.state = Follower

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:	   args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	r.applyCh <- msg
	DPrintf("[LogCompaction] server(id: %d) apply snapshot(index: %d, term: %d)\n",
		r.id, args.LastIncludedIndex, args.LastIncludedTerm)
}

//	send InstallSnapshot RPC
func (r *Raft) sendInstallSnapshot(serverId int, args InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	serverAddress := strconv.Itoa(serverId)
	return r.peers[r.id].Call(serverAddress, "Raft.InstallSnapshot", args, reply)
}

/* -------------------- install snapshot: end -------------------- */
