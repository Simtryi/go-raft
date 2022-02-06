package raft

//	raft server state
type State uint32

//	state enum
const (
	Follower State = iota
	Candidate
	Leader
)

//	votedFor starts with 0, -1 means votedFor is null
const VOTENULL = -1

//	state string
func (s State) String() string {
	switch s {
	case Follower:
		return "follower"
	case Candidate:
		return "candidate"
	case Leader:
		return "leader"
	default:
		return "unknown"
	}
}



// candidate => follower: discovers current leader or new term
// leader => follower: discovers server with higher term
func (r *Raft) convertToFollower(term int) {
	defer r.save()
	DPrintf("[Raft] convert server(%v) state(%v => follower) term(%v => %v)\n",
		r.id, r.state.String(), r.currentTerm, term)

	r.state = Follower
	r.currentTerm = term
	r.votedFor = VOTENULL
}

//	follower => candidate: times out, starts election
//	candidate => candidate: times out, new election
func (r *Raft) convertToCandidate() {
	defer r.save()
	DPrintf("[Raft] convert server(%v) state(%v => candidate) term(%v => %v)\n",
		r.id, r.state.String(), r.currentTerm, r.currentTerm + 1)

	r.state = Candidate
	r.currentTerm++
	r.votedFor = r.id
}

//	candidate => leader: receives votes from the majority of servers
func (r *Raft) convertToLeader() {
	defer r.save()

	if r.state != Candidate {
		return
	}

	DPrintf("[Raft] convert server(%v) state(%v => leader) term(%v => %v)\n",
		r.id, r.state.String(), r.currentTerm, r.currentTerm)

	r.state = Leader
	//	reinitialized nextIndex and matchIndex after election
	r.nextIndex = make([]int, len(r.peers))
	r.matchIndex = make([]int, len(r.peers))
	for i := 0; i < len(r.peers); i++ {
		//	initialized to leader last log index + 1
		r.nextIndex[i] = r.getLastLogIndex() + 1
		//	initialized to 0
		r.matchIndex[i] = 0
	}
}
