package raft

//	log entry
type Entry struct {
	Index 	int			//	log entry's position in the log
	Term 	int			//	term number, detect inconsistencies between logs
	Command interface{}	//	client command
}



func (r *Raft) getLastIncludedIndex() int {
	return r.lastIncludedIndex
}

func (r *Raft) getLastIncludedTerm() int {
	return r.lastIncludedTerm
}

//	get index of server's last log entry
func (r *Raft) getLastLogIndex() int {
	lastIncludedIndex := r.getLastIncludedIndex()
	return lastIncludedIndex + len(r.log) - 1
}

//	get term of server's last log entry
func (r *Raft) getLastLogTerm() int {
	return r.log[len(r.log) - 1].Term
}

//	get index of log entry immediately preceding new ones
func (r *Raft) getPrevLogIndex(id int) int {
	return r.nextIndex[id] - 1
}

//	get term of prevLogIndex entry
func (r *Raft) getPrevLogTerm(id int) int {
	lastIncludedIndex := r.getLastIncludedIndex()
	prevLogIndex := r.getPrevLogIndex(id)
	return r.log[prevLogIndex - lastIncludedIndex].Term
}




type ApplyMsg struct {
	CommandValid	bool
	Command 		interface{}
	CommandIndex	int

	SnapshotValid	bool
	Snapshot		[]byte
	SnapshotTerm	int
	SnapshotIndex	int
}

//	if commitIndex > lastApplied,
//	increment lastApplied,
//	apply log[lastApplied] to state machine
func (r *Raft) applyLogs() {
	for {
		select {
		case <- r.commitCh:
			r.mu.Lock()
			commitIndex := r.commitIndex
			lastApplied := r.lastApplied
			r.mu.Unlock()

			for commitIndex > lastApplied {
				r.mu.Lock()
				r.lastApplied++
				lastApplied = r.lastApplied

				entry := r.log[r.lastApplied - r.getLastIncludedIndex()]
				msg := ApplyMsg{
					CommandValid: true,
					Command: 	  entry.Command,
					CommandIndex: entry.Index,
				}
				r.mu.Unlock()

				r.applyCh <- msg
				DPrintf("[ApplyLogs] server(%d) apply entry(index: %d, command: %v)\n", r.id, entry.Index, entry.Command)
			}
		}
	}
}