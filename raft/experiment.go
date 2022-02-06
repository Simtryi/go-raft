package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"raft/rpc"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

type experiment struct {
	mu        			sync.Mutex
	t         			*testing.T
	n         			int						//	num of raft servers

	network   			*rpc.Network
	enabled 			[]bool  				//	whether server is enabled, hostId => bool
	online   			[]bool 					// 	whether each server is on the network, hostId => bool

	hosts				[]*rpc.Host
	persisters  		[]*Persister
	rafts     			[]*Raft

	logs      			[]map[int]interface{} 	//	raftId: {commandIndex: command}
	applyErrs  			[]string 				// 	from apply channel readers

	start     			time.Time             	// 	time at which MakeExperiment() was called
	t0        			time.Time 				// 	time at which raft_test.go called e.begin()

	total0     			int       				// 	Total() at start of test
	bytes0    			int64					//	Bytes() at start of test
	commands0    		int      				// 	number of agreements
	maxCommandIndex0 	int
	maxCommandIndex  	int
}

const SnapShotInterval = 10

var cpuOnce sync.Once

func MakeExperiment(t *testing.T, n int, isReliable bool, useSnapshot bool) *experiment {
	cpuOnce.Do(func() {
		if runtime.NumCPU() < 2 {
			fmt.Printf("[Experiment] warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(MakeSeed())
	})
	runtime.GOMAXPROCS(4)

	e := new(experiment)
	e.t = t
	e.n = n

	e.network = rpc.MakeNetwork()
	e.enabled = make([]bool, n)
	e.online = make([]bool, n)

	e.hosts = make([]*rpc.Host, n)
	e.persisters = make([]*Persister, n)
	e.rafts = make([]*Raft, n)

	e.logs = make([]map[int]interface{}, e.n)
	e.applyErrs = make([]string, e.n)

	e.start = time.Now()

	e.setReliable(isReliable)
	e.setDelay(true)

	applier := e.applier
	if useSnapshot {
		applier = e.applierSnap
	}

	// 	create a full set of Rafts
	for i := 0; i < e.n; i++ {
		e.logs[i] = make(map[int]interface{})
		e.startRaft(i, applier)
	}

	// 	online everyone
	for i := 0; i < e.n; i++ {
		e.onlineHost(i)
	}

	return e
}

// 	start a Test, print the Test message
func (e *experiment) begin(description string) {
	fmt.Printf("%s ...\n", description)
	e.t0 = time.Now()
	e.total0 = e.GetTotal()
	e.bytes0 = e.GetBytes()
	e.commands0 = 0
	e.maxCommandIndex0 = e.maxCommandIndex
}

// 	end a Test，print the Passed message, and some performance numbers
func (e *experiment) end() {
	e.checkTimeout()
	if e.t.Failed() == false {
		e.mu.Lock()
		t := time.Since(e.t0).Seconds()     				//	real time
		n := e.n											//	number of Raft peers
		total := e.GetTotal() - e.total0   					//	number of RPC sends
		bytes := e.GetBytes() - e.bytes0 					//	number of bytes
		commands := e.maxCommandIndex - e.maxCommandIndex0  //	number of Raft agreements reported
		e.mu.Unlock()

		fmt.Println("... Passed")
		fmt.Printf("time: %4.1fs, peers: %d, total: %4d, bytes: %7d, commands: %4d\n", t, n, total, bytes, commands)
		fmt.Println()
	}
}

func (e *experiment) cleanup() {
	for _, raft := range e.rafts {
		if raft != nil {
			raft.Exit()
		}
	}
	e.network.Exit()
	e.checkTimeout()
}

//	enforce a two minutes real-time limit on each test
func (e *experiment) checkTimeout() {
	if !e.t.Failed() && time.Since(e.start) > 120 * time.Second {
		e.t.Fatal("[Experiment] test took longer than 120 seconds")
	}
}



/* -------------------- network: begin -------------------- */

//	start host
func (e *experiment) startHost(id int) {
	DPrintf("[Experiment] start host(%d)\n", id)
	e.enabled[id] = true

	hostAddress := strconv.Itoa(id)
	e.network.Start(hostAddress)
}

//	crash host
func (e *experiment) crashHost(id int) {
	DPrintf("[Experiment] crash host(%d)\n", id)
	e.enabled[id] = false

	hostAddress := strconv.Itoa(id)
	e.network.Crash(hostAddress)
}

//	delete host
func (e *experiment) deleteHost(id int) {
	DPrintf("[Experiment] delete host(%d)\n", id)

	hostAddress := strconv.Itoa(id)
	e.network.Delete(hostAddress)
}

// 	connect host to the network
func (e *experiment) onlineHost(id int) {
	DPrintf("[Experiment] connect host(%d) to the network\n", id)
	e.online[id] = true

	hostAddress := strconv.Itoa(id)
	for i := 0; i < e.n; i++ {
		if i == id {
			continue
		}

		if e.online[i] {
			e.network.Connect(hostAddress, strconv.Itoa(i))
			e.network.Connect(strconv.Itoa(i), hostAddress)
		}
	}
}

// 	disconnect host from the network
func (e *experiment) offlineHost(id int) {
	DPrintf("[Experiment] disconnect host(%d) from the network\n", id)
	e.online[id] = false

	hostAddress := strconv.Itoa(id)
	for i := 0; i < e.n; i++ {
		if i == id {
			continue
		}

		e.network.Disconnect(hostAddress, strconv.Itoa(i))
		e.network.Disconnect(strconv.Itoa(i), hostAddress)
	}
}

func (e *experiment) setReliable(isReliable bool) {
	e.network.SetReliable(isReliable)
}

func (e *experiment) setDelay(isDelay bool) {
	e.network.SetDelay(isDelay)
}

func (e *experiment) setLongReordering(isLongReordering bool) {
	e.network.SetLongReordering(isLongReordering)
}

/* -------------------- network: end -------------------- */



/* -------------------- statistical: begin -------------------- */

func (e *experiment) GetCount(serverAddress string) int {
	return e.network.GetCount(serverAddress)
}

func (e *experiment) GetTotal() int {
	return e.network.GetTotal()
}

func (e *experiment) GetBytes() int64 {
	return e.network.GetBytes()
}

/* -------------------- statistical: end -------------------- */



/* -------------------- raft: begin -------------------- */

//	start or re-start a Raft
func (e *experiment) startRaft(id int, applier func(int, chan ApplyMsg)) {
	e.crashRaft(id)

	e.mu.Lock()
	if e.persisters[id] != nil {
		e.persisters[id] = e.persisters[id].Copy()
	} else {
		e.persisters[id] = new(Persister)
	}
	e.mu.Unlock()

	// listen to messages from Raft indicating newly committed messages
	applyCh := make(chan ApplyMsg)
	go applier(id, applyCh)

	raft := Make(id, e.hosts, e.persisters[id], applyCh)
	host := e.network.MakeHost(strconv.Itoa(id))

	e.mu.Lock()
	e.rafts[id] = raft
	e.hosts[id] = host
	e.mu.Unlock()

	e.startHost(id)
	host.Register(raft)
	host.Listen()
}

//	shut down a Raft server but save its persistent state
func (e *experiment) crashRaft(id int) {
	e.crashHost(id)
	e.offlineHost(id)
	e.deleteHost(id)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.persisters[id] != nil {
		e.persisters[id] = e.persisters[id].Copy()
	}

	raft := e.rafts[id]
	if raft != nil {
		e.mu.Unlock()
		raft.Exit()
		e.mu.Lock()
		e.rafts[id] = nil
	}

	if e.persisters[id] != nil {
		state := e.persisters[id].ReadState()
		snapshot := e.persisters[id].ReadSnapshot()
		e.persisters[id] = new(Persister)
		e.persisters[id].SaveStateAndSnapshot(state, snapshot)
	}
}

//	applier reads message form apply ch and checks that they match the log contents
func (e *experiment) applier(id int, applyCh chan ApplyMsg) {
	for msg := range applyCh {
		if msg.CommandValid {
			e.mu.Lock()
			errMsg, prevExist := e.checkLogs(id, msg)
			e.mu.Unlock()

			if msg.CommandIndex > 1 && prevExist == false {
				errMsg = fmt.Sprintf("[Experiment] server(%d) apply command(%d) out of order", id, msg.CommandIndex)
			}

			if errMsg != "" {
				e.applyErrs[id] = errMsg
				log.Fatalf("%v\n", errMsg)
			}
		}
	}
}

//	periodically snapshot raft state
func (e *experiment) applierSnap(id int, applyCh chan ApplyMsg) {
	lastApplied := 0
	for msg := range applyCh {
		if msg.SnapshotValid {
			e.mu.Lock()
			if e.rafts[id].CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
				e.logs[id] = make(map[int]interface{})

				reader := bytes.NewBuffer(msg.Snapshot)
				dec := gob.NewDecoder(reader)
				var command int
				if err := dec.Decode(&command); err != nil {
					log.Fatalf("[Experiment] decode command err: %v\n", err.Error())
				}

				e.logs[id][msg.SnapshotIndex] = command
				lastApplied = msg.SnapshotIndex
			}
			e.mu.Unlock()
		} else if msg.CommandValid && msg.CommandIndex > lastApplied {
			e.mu.Lock()
			errMsg, prevExist := e.checkLogs(id, msg)
			e.mu.Unlock()

			if msg.CommandIndex > 1 && prevExist == false {
				errMsg = fmt.Sprintf("[Experiment] server(%d) apply command(%d) out of order", id, msg.CommandIndex)
			}

			if errMsg != "" {
				e.applyErrs[id] = errMsg
				log.Fatalf("%v\n", errMsg)
			}

			lastApplied = msg.CommandIndex
			if (msg.CommandIndex + 1) % SnapShotInterval == 0 {
				writer := new(bytes.Buffer)
				enc := gob.NewEncoder(writer)
				command := msg.Command
				if err := enc.Encode(command); err != nil {
					log.Fatalf("[Experiment] encode command err: %v\n", err.Error())
				}
				e.rafts[id].Snapshot(msg.CommandIndex, writer.Bytes())
			}
		} else {
			// Ignore other types of ApplyMsg or old
			// commands. Old command may never happen,
			// depending on the Raft implementation, but
			// just in case.
			// DPrintf("Ignore: Index %v lastApplied %v\n", m.CommandIndex, lastApplied)
		}
	}
}

// 	check that there's exactly one leader,
// 	try a few times in case re-elections are needed
func (e *experiment) checkOneLeader() int {
	for iters := 0; iters < 10; iters++ {
		ms := 450 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)

		leaderMap := make(map[int][]int)
		for i := 0; i < e.n; i++ {
			if e.enabled[i] && e.online[i] {
				if term, isLeader := e.rafts[i].GetState(); isLeader {
					leaderMap[term] = append(leaderMap[term], i)
				}
			}
		}

		lastTermWithLeader := -1
		for term, leaders := range leaderMap {
			if len(leaders) > 1 {
				e.t.Fatalf("[Experiment] term %d has %d (>1) leaders", term, len(leaders))
			}
			if term > lastTermWithLeader {
				lastTermWithLeader = term
			}
		}

		if len(leaderMap) != 0 {
			return leaderMap[lastTermWithLeader][0]
		}
	}

	e.t.Fatalf("[Experiment] expected one leader, got none")
	return -1
}

// 	check that there's no leader
func (e *experiment) checkNoLeader() {
	for i := 0; i < e.n; i++ {
		if e.enabled[i] && e.online[i]{
			_, isLeader := e.rafts[i].GetState()
			if isLeader {
				e.t.Fatalf("[Experiment] expected no leader, but raft(%d) claims to be leader", i)
			}
		}
	}
}

// 	check that everyone agrees on the term
func (e *experiment) checkTerms() int {
	term := -1
	for i := 0; i < e.n; i++ {
		if e.enabled[i] && e.online[i] {
			currTerm, _ := e.rafts[i].GetState()
			if term == -1 {
				term = currTerm
			} else if currTerm != term {
				e.t.Fatalf("[Experiment] servers disagree on term")
			}
		}
	}
	return term
}

func (e *experiment) checkLogs(id int, msg ApplyMsg) (errMsg string, prevExist bool) {
	command := msg.Command
	//	determines whether the command for the same index are the same
	for i := 0; i < len(e.logs); i++ {
		if currCommand, exist := e.logs[i][msg.CommandIndex]; exist && currCommand != command {
			// some server has already committed a different value for this entry
			errMsg = fmt.Sprintf("[Experiment] command at index(%d) are different, (server: %d, command: %v) != (server: %d, command: %v)",
				msg.CommandIndex, id, command, i, currCommand)
		}
	}

	_, prevExist = e.logs[id][msg.CommandIndex - 1]
	e.logs[id][msg.CommandIndex] = command
	e.maxCommandIndex = Max(e.maxCommandIndex, msg.CommandIndex)
	return
}

//	do a complete agreement
func (e *experiment) one(command interface{}, expectedCount int, retry bool) int {
	t0 := time.Now()

	id := 0
	for time.Since(t0).Seconds() < 10 {	// 	entirely gives up after about 10 seconds.
		commandIndex := -1

		// 	try all the rafts, maybe one is the leader
		for i := 0; i < e.n; i++ {
			id = (id + 1) % e.n
			var raft *Raft

			e.mu.Lock()
			if e.enabled[id] && e.online[id] {
				raft = e.rafts[id]
			}
			e.mu.Unlock()

			if raft != nil {
				index, _, isLeader := raft.Start(command)
				if isLeader {
					commandIndex = index
					break
				}
			}
		}

		if commandIndex != -1 {
			// 	somebody claimed to be the leader and to have submitted our command;
			//	wait a while for agreement
			t1 := time.Now()
			for time.Since(t1).Seconds() < 2 {
				count, currCommand := e.nCommitted(commandIndex)
				if count > 0 && count >= expectedCount {
					// 	command committed, and it was the command we submitted
					if currCommand == command {
						return commandIndex
					}
				}
				time.Sleep(20 * time.Millisecond)
			}

			if retry == false {
				e.t.Fatalf("[Experiment] command(%v) failed to reach agreement", command)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}

	e.t.Fatalf("[Experiment] command(%v) failed to reach agreement", command)
	return -1
}

// wait for at least n servers to commit, but don't wait forever
func (e *experiment) wait(commandIndex int, startTerm int, n int) interface{} {
	t0 := 10 * time.Millisecond

	for iters := 0; iters < 30; iters++ {
		count, _ := e.nCommitted(commandIndex)
		if count >= n {
			break
		}

		time.Sleep(t0)
		if t0 < time.Second {
			t0 *= 2
		}

		if startTerm > -1 {
			for _, raft := range e.rafts {
				if term, _ := raft.GetState(); term > startTerm {
					// someone has moved on
					// can no longer guarantee that we'll "win"
					return -1
				}
			}
		}
	}

	count, command := e.nCommitted(commandIndex)
	if count < n {
		e.t.Fatalf("[Experiment] only %d decided for commandIndex %d; wanted %d\n",
			count, commandIndex, n)
	}

	return command
}

//	how many servers think a command is committed at commandIndex
func (e *experiment) nCommitted(commandIndex int) (count int, command interface{}) {
	for i := 0; i < len(e.rafts); i++ {
		if e.applyErrs[i] != "" {
			e.t.Fatal(e.applyErrs[i])
		}

		e.mu.Lock()
		currCommand, exist := e.logs[i][commandIndex]
		e.mu.Unlock()

		if exist {
			if count > 0 && currCommand != command {
				e.t.Fatalf("[Experiment] committed command do not match: (index: %d, currCommand: %v, command: %v)\n",
					commandIndex, currCommand, command)
			}
			count++
			command = currCommand
		}
	}
	return
}

//	maximum log size across all servers
func (e *experiment) getLogSize() int {
	logSize := 0
	for i := 0; i < e.n; i++ {
		n := e.persisters[i].SizeState()
		if n > logSize {
			logSize = n
		}
	}
	return logSize
}

/* -------------------- raft: end -------------------- */

