package rpc

import (
	"fmt"
	"log"
	"runtime"
	"strconv"
	"testing"
	"time"
)

type TestArgs struct {
	Num1, Num2 int
}

type TestReply struct {
	Sum int
}

type Test struct {

}

func (t *Test) Add(args TestArgs, reply *TestReply) {
	reply.Sum = args.Num1 + args.Num2
}



func TestBasic(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")
	network.Start("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	network.Connect("client1", "server1")

	server.Register(test)
	server.Listen()

	args := TestArgs{1, 2}
	reply := new(TestReply)
	client.Call("server1", "Test.Add", args, reply)

	if reply.Sum != 3 {
		t.Fatalf("wrong reply from Add()")
	}
}

func TestEnable(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	network.Connect("client1", "server1")

	server.Register(test)
	server.Listen()

	args := TestArgs{1, 2}
	reply := new(TestReply)

	{
		client.Call("server1", "Test.Add", args, &reply)
		if reply.Sum != 0 {
			t.Fatalf("unexpected reply from Add()")
		}
	}

	network.Start("client1")

	{
		client.Call("server1", "Test.Add", args, &reply)
		if reply.Sum != 3 {
			t.Fatalf("wrong reply from Add()")
		}
	}
}

func TestDisconnect(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")
	network.Start("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	server.Register(test)
	server.Listen()

	args := TestArgs{1, 2}
	reply := new(TestReply)

	{
		client.Call("server1", "Test.Add", args, &reply)
		if reply.Sum != 0 {
			t.Fatalf("unexpected reply from Add()")
		}
	}

	network.Connect("client1", "server1")

	{
		client.Call("server1", "Test.Add", args, &reply)
		if reply.Sum != 3 {
			t.Fatalf("wrong reply from Add()")
		}
	}
}

func TestTotal(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")
	network.Start("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	network.Connect("client1", "server1")

	server.Register(test)
	server.Listen()

	for i := 0; i < 10; i++ {
		args := TestArgs{i, i * i}
		reply := new(TestReply)
		client.Call("server1", "Test.Add", args, reply)

		wanted := i + i * i
		if reply.Sum != wanted {
			t.Fatalf("wrong reply %v from Add(), expecting %v", reply.Sum, wanted)
		}
	}

	total := network.GetTotal()
	if total != 10 {
		t.Fatalf("wrong Total() %v, expected 10\n", total)
	}
}

func TestUnreliable(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	network.SetReliable(false)
	defer network.Exit()

	server := network.MakeHost("server1")
	network.Start("server1")

	server.Register(test)
	server.Listen()

	ch := make(chan int)

	clientNum := 300
	for i := 0; i < clientNum; i++ {
		go func(id int) {
			n := 0
			defer func() { ch <- n }()

			clientName := "client" + strconv.Itoa(id)
			client := network.MakeHost(clientName)
			network.Start(clientName)
			network.Connect(clientName, "server1")

			args := TestArgs{id, id * id}
			reply := new(TestReply)
			ok := client.Call("server1", "Test.Add", args, reply)
			if ok {
				wanted := id + id * id
				if reply.Sum != wanted {
					t.Fatalf("wrong reply %v from Add(), expecting %v", reply.Sum, wanted)
				}
				n++
			}
		}(i)
	}

	total := 0
	for i := 0; i < clientNum; i++ {
		x := <- ch
		total += x
	}

	if total == clientNum || total == 0 {
		t.Fatalf("all RPCs succeeded despite unreliable")
	}
}

func TestConcurrentMany(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	server := network.MakeHost("server1")
	network.Start("server1")

	server.Register(test)
	server.Listen()

	ch := make(chan int)

	clientNum := 20
	rpcNum := 10
	for i := 0; i < clientNum; i++ {
		go func(id int) {
			n := 0
			defer func() { ch <- n }()

			clientName := "client" + strconv.Itoa(id)
			client := network.MakeHost(clientName)
			network.Start(clientName)
			network.Connect(clientName, "server1")

			for j := 0; j < rpcNum; j++ {
				args := TestArgs{id, id * id}
				reply := new(TestReply)
				ok := client.Call("server1", "Test.Add", args, reply)
				if ok {
					wanted := id + id * id
					if reply.Sum != wanted {
						t.Fatalf("wrong reply %v from Add(), expecting %v", reply.Sum, wanted)
					}
					n++
				}
			}
		}(i)
	}

	total := 0
	for i := 0; i < clientNum; i++ {
		x := <- ch
		total += x
	}

	if total != clientNum * rpcNum {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, clientNum * rpcNum)
	}

	n := int(network.GetTotal())
	if total != n {
		t.Fatalf("wrong Total() %v, expected %v\n", total, n)
	}
}

func TestConcurrentOne(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")
	network.Start("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	network.Connect("client1", "server1")

	server.Register(test)
	server.Listen()

	ch := make(chan int)

	rpcNum := 20
	for i := 0; i < rpcNum; i++ {
		go func(id int) {
			n := 0
			defer func(){ ch <- n }()

			args := TestArgs{id, id * id}
			reply := new(TestReply)
			client.Call("server1", "Test.Add", args, reply)

			wanted := id + id * id
			if reply.Sum != wanted {
				t.Fatalf("wrong reply %v from Add(), expecting %v", reply.Sum, wanted)
			}
			n++
		}(i)
	}

	total := 0
	for i := 0; i < rpcNum; i++ {
		x := <- ch
		total += x
	}

	if total != rpcNum {
		t.Fatalf("wrong number of RPCs completed, got %v, expected %v", total, rpcNum)
	}

	n := network.GetCount("server1")
	if n != total {
		t.Fatalf("wrong Count() %v, expected %v\n", n, total)
	}
}

func TestBenchmark(t *testing.T) {
	log.SetFlags(0)
	runtime.GOMAXPROCS(4)

	test := new(Test)

	network := MakeNetwork()
	defer network.Exit()

	client := network.MakeHost("client1")
	network.Start("client1")

	server := network.MakeHost("server1")
	network.Start("server1")

	network.Connect("client1", "server1")

	server.Register(test)
	server.Listen()

	t0 := time.Now()
	n := 100000
	for i := 0; i < n; i++ {
		args := TestArgs{1, 1}
		reply := new(TestReply)
		client.Call("server1", "Test.Add", args, reply)
		if reply.Sum != 2 {
			t.Fatalf("wrong reply from Add")
		}
	}
	fmt.Printf("%v for %v\n", time.Since(t0), n)
}
