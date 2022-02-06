package raft

import (
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const RaftElectionTimeout = 1000 * time.Millisecond

const MaxLogSize = 2000

func TestInitialElectionA(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test A: initial election")

	//	is a leader elected?
	e.checkOneLeader()

	//	sleep a bit to avoid racing with followers learning of the
	//	election, then check that all peers agree on the term
	time.Sleep(50 * time.Millisecond)
	term1 := e.checkTerms()
	if term1 < 1 {
		t.Fatalf("term is %v, but should be at least 1", term1)
	}

	//	does the leader + term stay the same if there is no network failure?
	time.Sleep(2 * RaftElectionTimeout)
	term2 := e.checkTerms()
	if term1 != term2 {
		fmt.Printf("warning: term changed even though there were no failures")
	}

	//	there should still be a leader.
	e.checkOneLeader()

	e.end()
}

func TestReElectionA(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test A: election after network failure")

	leader1 := e.checkOneLeader()

	//	if the leader disconnects, a new one should be elected
	e.offlineHost(leader1)
	e.checkOneLeader()

	// 	if the old leader rejoins, that shouldn't disturb the new leader
	e.onlineHost(leader1)
	leader2 := e.checkOneLeader()

	// 	if there's no quorum, no leader should be elected
	e.offlineHost(leader2)
	e.offlineHost((leader2 + 1) % servers)
	time.Sleep(2 * RaftElectionTimeout)
	e.checkNoLeader()

	// 	if a quorum arises, it should elect a leader
	e.onlineHost((leader2 + 1) % servers)
	e.checkOneLeader()

	// 	re-join of last node shouldn't prevent leader from existing
	e.onlineHost(leader2)
	e.checkOneLeader()

	e.end()
}

func TestBasicAgreeB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: basic agreement")

	for commandIndex := 1; commandIndex <= 3; commandIndex++ {
		count, _ := e.nCommitted(commandIndex)
		if count > 0 {
			t.Fatalf("some have committed before Start()")
		}

		index := e.one("x <- 1", servers, false)
		if index != commandIndex {
			t.Fatalf("got index %v but expected %v", index, commandIndex)
		}
	}

	e.end()
}

func TestFailAgreeB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: agreement despite follower disconnection")

	e.one("x <- 1", servers, false)

	//	disconnect one follower from the network
	leader := e.checkOneLeader()
	e.offlineHost((leader + 1) % servers)

	//	the leader and remaining follower should be
	//	able to agree despite the disconnected follower
	e.one("x <- 2", servers - 1, false)
	e.one("x <- 3", servers - 1, false)
	time.Sleep(RaftElectionTimeout)
	e.one("y <- 1", servers - 1, false)
	e.one("y <- 2", servers - 1, false)

	//	re-connect
	e.onlineHost((leader + 1) % servers)

	//	the full set of servers should preserve
	//	previous agreements, and be able to agree
	//	on new commands
	e.one("y <- 3", servers, true)
	time.Sleep(RaftElectionTimeout)
	e.one("x <- 10", servers, true)

	e.end()
}

func TestFailNoAgreeB(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: no agreement if too many followers disconnect")

	e.one("x <- 1", servers, false)

	//	3 of 5 followers disconnect
	leader := e.checkOneLeader()
	e.offlineHost((leader + 1) % servers)
	e.offlineHost((leader + 2) % servers)
	e.offlineHost((leader + 3) % servers)

	commandIndex, _, isLeader := e.rafts[leader].Start("x <- 2")
	if isLeader != true {
		t.Fatalf("leader rejected Start()")
	}
	if commandIndex != 2 {
		t.Fatalf("expected index 2, got %v", commandIndex)
	}

	time.Sleep(2 * RaftElectionTimeout)

	count, _ := e.nCommitted(commandIndex)
	if count > 0 {
		t.Fatalf("%v committed but no majority", count)
	}

	//	repair
	e.onlineHost((leader + 1) % servers)
	e.onlineHost((leader + 2) % servers)
	e.onlineHost((leader + 3) % servers)

	//	the disconnected majority may have chosen a leader from
	//	among their own ranks, forgetting index 2
	leader2 := e.checkOneLeader()
	commandIndex2, _, isLeader2 := e.rafts[leader2].Start("x <- 3")
	if isLeader2 == false {
		t.Fatalf("leader2 regected Start()")
	}
	if commandIndex2 < 2 || commandIndex2 > 3 {
		t.Fatalf("unexpected index %v", commandIndex2)
	}

	e.one("y <- 1", servers, true)

	e.end()
}

func TestConcurrentStartsB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: concurrent Start()s")

	var success bool

	loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// 	give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader := e.checkOneLeader()
		_, term, isLeader := e.rafts[leader].Start(1)
		if !isLeader {
			// 	leader moved on really quickly
			continue
		}

		var wg sync.WaitGroup

		iters := 5
		commandIndexCh := make(chan int, iters)
		for i := 0; i < iters; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()

				commandIndex, term1, isLeader := e.rafts[leader].Start(100 + index)
				if term1 != term {
					return
				}
				if isLeader != true {
					return
				}

				commandIndexCh <- commandIndex
			}(i)
		}

		wg.Wait()
		close(commandIndexCh)

		for i := 0; i < servers; i++ {
			if t, _ := e.rafts[i].GetState(); t != term {
				// 	term changed -- can't expect low RPC counts
				continue loop
			}
		}

		failed := false
		commands := []int{}
		for commandIndex := range commandIndexCh {
			command := e.wait(commandIndex, servers, term)
			if currCommand, ok := command.(int); ok {
				if currCommand == -1 {
					// 	peers have moved on to later terms
					// 	so we can't expect all Start()s to
					// 	have succeeded
					failed = true
					break
				}
				commands = append(commands, currCommand)
			} else {
				t.Fatalf("value %v is not an int", command)
			}
		}

		if failed {
			// 	avoid leaking goroutines
			go func() {
				for range commandIndexCh {
				}
			}()
			continue
		}

		for i := 0; i < iters; i++ {
			wanted := 100 + i
			ok := false
			for j := 0; j < len(commands); j++ {
				if commands[j] == wanted {
					ok = true
				}
			}
			if ok == false {
				t.Fatalf("cmd %v missing in %v", wanted, commands)
			}
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	e.end()
}

func TestRejoinB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: rejoin of partitioned leader")

	e.one("x <- 1", servers, true)

	// 	leader network failure
	leader1 := e.checkOneLeader()
	e.offlineHost(leader1)

	// 	make old leader try to agree on some entries
	e.rafts[leader1].Start("x <- 2")
	e.rafts[leader1].Start("x <- 3")
	e.rafts[leader1].Start("y <- 1")

	// 	new leader commits, also for index=2
	e.one("y <- 2", servers - 1, true)

	// 	new leader network failure
	leader2 := e.checkOneLeader()
	e.offlineHost(leader2)

	//	old leader connected again
	e.onlineHost(leader1)

	e.one("y <- 2", servers - 1, true)

	// 	all together now
	e.onlineHost(leader2)

	e.one("y <- 3", servers, true)

	e.end()
}

func TestBackupB(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: leader backs up quickly over incorrect follower logs")

	e.one(rand.Int(), servers, true)

	// 	put leader and one follower in a partition
	leader1 := e.checkOneLeader()
	e.offlineHost((leader1 + 2) % servers)
	e.offlineHost((leader1 + 3) % servers)
	e.offlineHost((leader1 + 4) % servers)

	// 	submit lots of commands that won't commit
	for i := 0; i < 50; i++ {
		e.rafts[leader1].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	e.offlineHost((leader1 + 0) % servers)
	e.offlineHost((leader1 + 1) % servers)

	// 	allow other partition to recover
	e.onlineHost((leader1 + 2) % servers)
	e.onlineHost((leader1 + 3) % servers)
	e.onlineHost((leader1 + 4) % servers)

	// 	lots of successful commands to new group
	for i := 0; i < 50; i++ {
		e.one(rand.Int(), 3, true)
	}

	// 	now another partitioned leader and one follower
	leader2 := e.checkOneLeader()
	other := (leader1 + 2) % servers
	if leader2 == other {
		other = (leader2 + 1) % servers
	}
	e.offlineHost(other)

	// 	lots more commands that won't commit
	for i := 0; i < 50; i++ {
		e.rafts[leader2].Start(rand.Int())
	}

	time.Sleep(RaftElectionTimeout / 2)

	// 	bring original leader back to life,
	for i := 0; i < servers; i++ {
		e.offlineHost(i)
	}
	e.onlineHost((leader1 + 0) % servers)
	e.onlineHost((leader1 + 1) % servers)
	e.onlineHost(other)

	// 	lots of successful commands to new group.
	for i := 0; i < 50; i++ {
		e.one(rand.Int(), 3, true)
	}

	// 	now everyone
	for i := 0; i < servers; i++ {
		e.onlineHost(i)
	}
	e.one(rand.Int(), servers, true)

	e.end()
}

func TestCountB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: RPC counts aren't too high")

	rpcs := func() (n int) {
		for j := 0; j < servers; j++ {
			n += e.GetCount(strconv.Itoa(j))
		}
		return
	}

	leader := e.checkOneLeader()

	total1 := rpcs()

	if total1 > 30 || total1 < 1 {
		t.Fatalf("too many or few RPCs (%v) to elect initial leader\n", total1)
	}

	var total2 int
	var success bool

	loop:
	for try := 0; try < 5; try++ {
		if try > 0 {
			// 	give solution some time to settle
			time.Sleep(3 * time.Second)
		}

		leader = e.checkOneLeader()
		total1 = rpcs()

		iters := 10
		starti, term, ok := e.rafts[leader].Start(1)
		if !ok {
			// 	leader moved on really quickly
			continue
		}
		cmds := []int{}
		for i := 1; i < iters + 2; i++ {
			x := int(rand.Int31())
			cmds = append(cmds, x)
			index1, term1, ok := e.rafts[leader].Start(x)
			if term1 != term {
				// 	Term changed while starting
				continue loop
			}
			if !ok {
				// 	No longer the leader, so term has changed
				continue loop
			}
			if starti+i != index1 {
				t.Fatalf("Start() failed")
			}
		}

		for i := 1; i < iters+1; i++ {
			cmd := e.wait(starti+i, servers, term)
			if ix, ok := cmd.(int); ok == false || ix != cmds[i - 1] {
				if ix == -1 {
					// 	term changed -- try again
					continue loop
				}
				t.Fatalf("wrong value %v committed for index %v; expected %v\n", cmd, starti + i, cmds)
			}
		}

		failed := false
		total2 = 0
		for j := 0; j < servers; j++ {
			if t, _ := e.rafts[j].GetState(); t != term {
				// term changed -- can't expect low RPC counts
				// need to keep going to update total2
				failed = true
			}
			total2 += e.GetCount(strconv.Itoa(j))
		}

		if failed {
			continue loop
		}

		if total2 - total1 > (iters + 1 + 3) * 3 {
			t.Fatalf("too many RPCs (%v) for %v entries\n", total2 - total1, iters)
		}

		success = true
		break
	}

	if !success {
		t.Fatalf("term changed too often")
	}

	time.Sleep(RaftElectionTimeout)

	total3 := 0
	for j := 0; j < servers; j++ {
		total3 += e.GetCount(strconv.Itoa(j))
	}

	if total3 - total2 > 3 * 20 {
		t.Fatalf("too many RPCs (%v) for 1 second of idleness\n", total3 - total2)
	}

	e.end()
}

func TestBytesB(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test B: RPC byte count")

	e.one(99, servers, false)
	bytes0 := e.GetBytes()

	iters := 10
	var sent int64 = 0
	for index := 2; index < iters + 2; index++ {
		cmd := RandString(5000)
		xindex := e.one(cmd, servers, false)
		if xindex != index {
			t.Fatalf("got index %v but expected %v", xindex, index)
		}
		sent += int64(len(cmd))
	}

	bytes1 := e.GetBytes()
	got := bytes1 - bytes0
	expected := int64(servers) * sent
	if got > expected + 50000 {
		t.Fatalf("too many RPC bytes; got %v, expected %v", got, expected)
	}

	e.end()
}

func TestPersist1C(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test C: basic persistence")

	e.one(11, servers, true)

	// 	crash and re-start all
	for i := 0; i < servers; i++ {
		e.startRaft(i, e.applier)
	}
	for i := 0; i < servers; i++ {
		e.offlineHost(i)
		e.onlineHost(i)
	}

	e.one(12, servers, true)

	leader1 := e.checkOneLeader()
	e.offlineHost(leader1)
	e.startRaft(leader1, e.applier)
	e.onlineHost(leader1)

	e.one(13, servers, true)

	leader2 := e.checkOneLeader()
	e.offlineHost(leader2)
	e.one(14, servers - 1, true)
	e.startRaft(leader2, e.applier)
	e.onlineHost(leader2)

	e.wait(4, servers, -1) // 	wait for leader2 to join before killing i3

	i3 := (e.checkOneLeader() + 1) % servers
	e.offlineHost(i3)
	e.one(15, servers - 1, true)
	e.startRaft(i3, e.applier)
	e.onlineHost(i3)

	e.one(16, servers, true)

	e.end()
}

func TestPersist2C(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test C: more persistence")

	index := 1
	for iters := 0; iters < 5; iters++ {
		e.one(10 + index, servers, true)
		index++

		leader1 := e.checkOneLeader()

		e.offlineHost((leader1 + 1) % servers)
		e.offlineHost((leader1 + 2) % servers)

		e.one(10 + index, servers - 2, true)
		index++

		e.offlineHost((leader1 + 0) % servers)
		e.offlineHost((leader1 + 3) % servers)
		e.offlineHost((leader1 + 4) % servers)

		e.startRaft((leader1 + 1) % servers, e.applier)
		e.startRaft((leader1 + 2) % servers, e.applier)
		e.onlineHost((leader1 + 1) % servers)
		e.onlineHost((leader1 + 2) % servers)

		time.Sleep(RaftElectionTimeout)

		e.startRaft((leader1 + 3) % servers, e.applier)
		e.onlineHost((leader1 + 3) % servers)

		e.one(10 + index, servers - 2, true)
		index++

		e.onlineHost((leader1 + 4) % servers)
		e.onlineHost((leader1 + 0) % servers)
	}

	e.one(1000, servers, true)

	e.end()
}

func TestPersist3C(t *testing.T) {
	log.SetFlags(0)

	servers := 3
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test C: partitioned leader and one follower crash, leader restarts")

	e.one(101, 3, true)

	leader := e.checkOneLeader()
	e.offlineHost((leader + 2) % servers)

	e.one(102, 2, true)

	e.crashRaft((leader + 0) % servers)
	e.crashRaft((leader + 1) % servers)
	e.onlineHost((leader + 2) % servers)
	e.startRaft((leader + 0) % servers, e.applier)
	e.onlineHost((leader + 0) % servers)

	e.one(103, 2, true)

	e.startRaft((leader + 1) % servers, e.applier)
	e.onlineHost((leader + 1) % servers)

	e.one(104, servers, true)

	e.end()
}

func TestFigure8C(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, true, false)
	defer e.cleanup()

	e.begin("Test C: Figure 8")

	e.one(rand.Int(), 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		leader := -1
		for i := 0; i < servers; i++ {
			if e.rafts[i] != nil {
				_, _, ok := e.rafts[i].Start(rand.Int())
				if ok {
					leader = i
				}
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 {
			e.crashRaft(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if e.rafts[s] == nil {
				e.startRaft(s, e.applier)
				e.onlineHost(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if e.rafts[i] == nil {
			e.startRaft(i, e.applier)
			e.onlineHost(i)
		}
	}

	e.one(rand.Int(), servers, true)

	e.end()
}

func TestFigure8UnreliableC(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, false, false)
	defer e.cleanup()

	e.begin("Test C: Figure 8 (unreliable)")

	e.one(rand.Int()%10000, 1, true)

	nup := servers
	for iters := 0; iters < 1000; iters++ {
		if iters == 200 {
			e.setLongReordering(true)
		}
		leader := -1
		for i := 0; i < servers; i++ {
			_, _, ok := e.rafts[i].Start(rand.Int() % 10000)
			if ok && e.online[i] {
				leader = i
			}
		}

		if (rand.Int() % 1000) < 100 {
			ms := rand.Int63() % (int64(RaftElectionTimeout/time.Millisecond) / 2)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		} else {
			ms := rand.Int63() % 13
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}

		if leader != -1 && (rand.Int()%1000) < int(RaftElectionTimeout/time.Millisecond)/2 {
			e.offlineHost(leader)
			nup -= 1
		}

		if nup < 3 {
			s := rand.Int() % servers
			if e.online[s] == false {
				e.onlineHost(s)
				nup += 1
			}
		}
	}

	for i := 0; i < servers; i++ {
		if e.online[i] == false {
			e.onlineHost(i)
		}
	}

	e.one(rand.Int()%10000, servers, true)

	e.end()
}

func TestUnreliableAgreeC(t *testing.T) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, false, false)
	defer e.cleanup()

	e.begin("Test C: unreliable agreement")

	var wg sync.WaitGroup

	for iters := 1; iters < 50; iters++ {
		for j := 0; j < 4; j++ {
			wg.Add(1)
			go func(iters, j int) {
				defer wg.Done()
				e.one((100*iters)+j, 1, true)
			}(iters, j)
		}
		e.one(iters, 1, true)
	}

	e.setReliable(false)

	wg.Wait()

	e.one(100, servers, true)

	e.end()
}

func TestReliableChurnC(t *testing.T) {
	internalChurn(t, true)
}

func TestUnreliableChurnC(t *testing.T) {
	internalChurn(t, false)
}

func internalChurn(t *testing.T, reliable bool) {
	log.SetFlags(0)

	servers := 5
	e := MakeExperiment(t, servers, reliable, false)
	defer e.cleanup()

	if reliable {
		e.begin("Test C: churn")
	} else {
		e.begin("Test C: unreliable churn")
	}

	stop := int32(0)

	// create concurrent clients
	cfn := func(me int, ch chan []int) {
		var ret []int
		ret = nil
		defer func() { ch <- ret }()
		values := []int{}
		for atomic.LoadInt32(&stop) == 0 {
			x := rand.Int()
			index := -1
			ok := false
			for i := 0; i < servers; i++ {
				// try them all, maybe one of them is a leader
				e.mu.Lock()
				rf := e.rafts[i]
				e.mu.Unlock()
				if rf != nil {
					index1, _, ok1 := rf.Start(x)
					if ok1 {
						ok = ok1
						index = index1
					}
				}
			}
			if ok {
				// maybe leader will commit our value, maybe not.
				// but don't wait forever.
				for _, to := range []int{10, 20, 50, 100, 200} {
					nd, cmd := e.nCommitted(index)
					if nd > 0 {
						if xx, ok := cmd.(int); ok {
							if xx == x {
								values = append(values, x)
							}
						} else {
							e.t.Fatalf("wrong command type")
						}
						break
					}
					time.Sleep(time.Duration(to) * time.Millisecond)
				}
			} else {
				time.Sleep(time.Duration(79+me*17) * time.Millisecond)
			}
		}
		ret = values
	}

	ncli := 3
	cha := []chan []int{}
	for i := 0; i < ncli; i++ {
		cha = append(cha, make(chan []int))
		go cfn(i, cha[i])
	}

	for iters := 0; iters < 20; iters++ {
		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			e.offlineHost(i)
		}

		if (rand.Int() % 1000) < 500 {
			i := rand.Int() % servers
			if e.rafts[i] == nil {
				e.startRaft(i, e.applier)
			}
			e.onlineHost(i)
		}

		if (rand.Int() % 1000) < 200 {
			i := rand.Int() % servers
			if e.rafts[i] != nil {
				e.crashRaft(i)
			}
		}

		// Make crash/restart infrequent enough that the peers can often
		// keep up, but not so infrequent that everything has settled
		// down from one change to the next. Pick a value smaller than
		// the election timeout, but not hugely smaller.
		time.Sleep((RaftElectionTimeout * 7) / 10)
	}

	time.Sleep(RaftElectionTimeout)
	e.setReliable(false)
	for i := 0; i < servers; i++ {
		if e.rafts[i] == nil {
			e.startRaft(i, e.applier)
		}
		e.onlineHost(i)
	}

	atomic.StoreInt32(&stop, 1)

	values := []int{}
	for i := 0; i < ncli; i++ {
		vv := <-cha[i]
		if vv == nil {
			t.Fatal("client failed")
		}
		values = append(values, vv...)
	}

	time.Sleep(RaftElectionTimeout)

	lastIndex := e.one(rand.Int(), servers, true)

	really := make([]int, lastIndex+1)
	for index := 1; index <= lastIndex; index++ {
		v := e.wait(index, servers, -1)
		if vi, ok := v.(int); ok {
			really = append(really, vi)
		} else {
			t.Fatalf("not an int")
		}
	}

	for _, v1 := range values {
		ok := false
		for _, v2 := range really {
			if v1 == v2 {
				ok = true
			}
		}
		if ok == false {
			e.t.Fatalf("didn't find a value")
		}
	}

	e.end()
}

func TestSnapshotBasicD(t *testing.T) {
	snapCommon(t, "Test D: snapshots basic", false, true, false)
}

func TestSnapshotInstallD(t *testing.T) {
	snapCommon(t, "Test D: install snapshots (disconnect)", true, true, false)
}

func TestSnapshotInstallUnreliableD(t *testing.T) {
	snapCommon(t, "Test D: install snapshots (disconnect+unreliable)", true, false, false)
}

func TestSnapshotInstallCrashD(t *testing.T) {
	snapCommon(t, "Test D: install snapshots (crash)", false, true, true)
}

func TestSnapshotInstallUnCrashD(t *testing.T) {
	snapCommon(t, "Test D: install snapshots (unreliable+crash)", false, false, true)
}

func snapCommon(t *testing.T, description string, isDisconnect bool, isReliable bool, isCrash bool) {
	log.SetFlags(0)

	iters := 30

	servers := 3
	e := MakeExperiment(t, servers, isReliable, true)
	defer e.cleanup()

	e.begin(description)

	e.one(rand.Int(), servers, true)
	leader1 := e.checkOneLeader()

	for i := 0; i < iters; i++ {
		victim := (leader1 + 1) % servers
		sender := leader1
		if i % 3 == 1 {
			sender = (leader1 + 1) % servers
			victim = leader1
		}

		if isDisconnect {
			e.offlineHost(victim)
			e.one(rand.Int(), servers - 1, true)
		}
		if isCrash {
			e.crashRaft(victim)
			e.one(rand.Int(), servers - 1, true)
		}

		// 	send enough to get a snapshot
		for j := 0; j < SnapShotInterval + 1; j++ {
			e.rafts[sender].Start(rand.Int())
		}

		//	let applier threads catch up with the Start()'s
		e.one(rand.Int(), servers - 1, true)

		if e.getLogSize() >= MaxLogSize {
			e.t.Fatalf("Log size too large")
		}

		if isDisconnect {
			// 	reconnect a follower, who maybe behind and
			// 	needs to receive a snapshot to catch up.
			e.onlineHost(victim)
			e.one(rand.Int(), servers, true)
			leader1 = e.checkOneLeader()
		}
		if isCrash {
			e.startRaft(victim, e.applierSnap)
			e.onlineHost(victim)
			e.one(rand.Int(), servers, true)
			leader1 = e.checkOneLeader()
		}
	}

	e.end()
}
