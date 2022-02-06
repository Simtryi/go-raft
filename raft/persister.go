package raft

import (
	"bytes"
	"encoding/gob"
	"sync"
)

type Persister struct {
	mu 			sync.Mutex
	state		[]byte		//	raft state
	snapshot	[]byte
}

func (p *Persister) SaveState(state []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = Clone(state)
}

func (p *Persister) ReadState() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Clone(p.state)
}

func (p *Persister) SizeState() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.state)
}



func (p *Persister) SaveSnapshot(snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.snapshot = Clone(snapshot)
}

func (p *Persister) ReadSnapshot() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return Clone(p.snapshot)
}

func (p *Persister) SizeSnapshot() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.snapshot)
}



//	save both Raft state and K/V snapshot as a single atomic action,
//	to help avoid them getting out of sync
func (p *Persister) SaveStateAndSnapshot(state []byte, snapshot []byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.state = Clone(state)
	p.snapshot = Clone(snapshot)
}

func (p *Persister) Copy() *Persister {
	p.mu.Lock()
	defer p.mu.Unlock()

	np := new(Persister)
	np.state = p.state
	np.snapshot = p.snapshot
	return np
}



//	restore previously persisted state
func (r *Raft) read() {
	state := r.persister.ReadState()

	if state == nil || len(state) < 1 {
		return
	}

	reader := bytes.NewBuffer(state)
	dec := gob.NewDecoder(reader)
	dec.Decode(&r.currentTerm)
	dec.Decode(&r.votedFor)
	dec.Decode(&r.log)
	dec.Decode(&r.lastIncludedIndex)
	dec.Decode(&r.lastIncludedTerm)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart
func (r *Raft) save() {
	state := r.encodeState()
	r.persister.SaveState(state)
}

func (r *Raft) encodeState() []byte {
	writer := new(bytes.Buffer)
	enc := gob.NewEncoder(writer)
	enc.Encode(r.currentTerm)
	enc.Encode(r.votedFor)
	enc.Encode(r.log)
	enc.Encode(r.lastIncludedIndex)
	enc.Encode(r.lastIncludedTerm)
	return writer.Bytes()
}






