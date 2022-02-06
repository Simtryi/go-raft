package rpc

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

//	rpc network
type Network struct {
	mu             		sync.Mutex
	hostMap				map[string]*Host			//	Host.address => Host
	enableMap	    	map[string]bool        		// 	Host.address => bool
	connectionMap   	map[string]bool 			// 	clientAddress + serverAddress => bool

	isReliable   		bool						//	network whether reliable
	isDelay     		bool                       	//	pause a long time on send on disabled connection
	isLongReordering 	bool						// 	sometimes delay replies a long time

	reqCh         		chan *request
	exitCh           	chan struct{} 				// 	closed when Network exit

	count				int32 						//	total RPC count, for statistics
	bytes 				int64 						//	total bytes send, for statistics
}

//	rpc request
type request struct {
	clientAddress 	string
	serverAddress	string
	serviceMethod  	string      	// 	serviceName and methodName, e.g. "Service.Method"
	args 			[]byte			//	rpc arguments
	resCh			chan *response
}

//	rpc response
type response struct {
	success			bool
	reply 			[]byte
}

//	create network
func MakeNetwork() *Network {
	network := new(Network)
	network.hostMap = make(map[string]*Host)
	network.enableMap = make(map[string]bool)
	network.connectionMap = make(map[string]bool)

	network.isReliable = true

	network.reqCh = make(chan *request)
	network.exitCh = make(chan struct{})

	go network.accept()

	return network
}

//	accept request
func (network *Network) accept() {
	for {
		select {
		case req := <- network.reqCh:
			atomic.AddInt32(&network.count, 1)
			atomic.AddInt64(&network.bytes, int64(len(req.args)))
			go network.process(req)
		case <- network.exitCh:
			DPrintf("[Network] exit network\n")
			return
		}
	}
}

//	process request
func (network *Network) process(req *request) {
	isReliable, isDelay, isLongReordering, clientEnable, connectable, server := network.info(req.clientAddress, req.serverAddress)

	if clientEnable && connectable && server != nil {
		//	network is unreliable
		if !isReliable {
			// short delay
			ms := rand.Int() % 27
			time.Sleep(time.Duration(ms) * time.Millisecond)

			if rand.Int() % 1000 < 100 {
				//	drop the request, return as if timeout
				req.resCh <- &response{false, nil}
				return
			}
		}

		//	handle the request
		eCh := make(chan *response)
		go func() {
			eCh <- server.dispatch(req)
		}()

		// wait for the response,
		// but stop waiting if DeleteServer() has been called,
		// and return an error
		var result *response

		resOk := false
		serverEnable := true
		for resOk == false && serverEnable == true {
			select {
			case result = <- eCh:
				resOk = true
			case <- time.After(100 * time.Millisecond):
				serverEnable = network.checkServer(req.serverAddress, server)
				if !serverEnable {
					go func() {
						//	drain channel to let the goroutine created earlier terminate
						<- eCh
					}()
				}
			}
		}

		serverEnable = network.checkServer(req.serverAddress, server)
		if resOk == false || serverEnable == false {
			// server was killed while we were waiting, return error
			req.resCh <- &response{false, nil}
		} else if !isReliable && (rand.Int() % 1000) < 100 {
			//	network is unreliable, drop the reply, return as if timeout
			req.resCh <- &response{false, nil}
		} else if isLongReordering && rand.Intn(900) < 600 {
			//	delay the response for a while
			ms := 200 + rand.Intn(1 + rand.Intn(2000))
			time.AfterFunc(time.Duration(ms) * time.Millisecond, func() {
				atomic.AddInt64(&network.bytes, int64(len(result.reply)))
				req.resCh <- result
			})
		} else {
			atomic.AddInt64(&network.bytes, int64(len(result.reply)))
			req.resCh <- result
		}
	} else {
		// simulate no reply and eventual timeout
		ms := 0
		if isDelay {
			ms = rand.Int() % 7000
		} else {
			ms = rand.Int() % 100
		}

		time.AfterFunc(time.Duration(ms) * time.Millisecond, func() {
			req.resCh <- &response{false, nil}
		})
	}
}

//	get network information
func (network *Network) info(clientAddress, serverAddress string) (isReliable, isDelay, isLongReordering bool,
	clientEnable, connectable bool, server *Host) {
	network.mu.Lock()
	defer network.mu.Unlock()

	isReliable = network.isReliable
	isDelay = network.isDelay
	isLongReordering = network.isLongReordering

	clientEnable = network.checkClient(clientAddress)

	key := clientAddress + "-" + serverAddress
	connectable = network.connectionMap[key]

	server = network.hostMap[serverAddress]

	return
}

//	check client whether enable
func (network *Network) checkClient(address string) bool {
	client := network.hostMap[address]

	if client == nil || !network.enableMap[address] {
		return false
	}

	return true
}

//	check server whether enable
func (network *Network) checkServer(address string, host *Host) bool {
	network.mu.Lock()
	defer network.mu.Unlock()

	server := network.hostMap[address]

	if server == nil || !network.enableMap[address] {
		return false
	}

	if !server.serverEnable || server != host {
		return false
	}

	return true
}



//	start or recover host
func (network *Network) Start(address string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.enableMap[address] = true
}

//	crash host
func (network *Network) Crash(address string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.enableMap[address] = false
}

//	delete host
func (network *Network) Delete(address string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	delete(network.hostMap, address)
}

//	connect client to server
func (network *Network) Connect(clientAddress, serverAddress string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	key := clientAddress + "-" + serverAddress
	network.connectionMap[key] = true
}

//	disconnect client to server
func (network *Network) Disconnect(clientAddress, serverAddress string) {
	network.mu.Lock()
	defer network.mu.Unlock()
	key := clientAddress + "-" + serverAddress
	network.connectionMap[key] = false
}



func (network *Network) SetReliable(isReliable bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.isReliable = isReliable
}

func (network *Network) SetDelay(isDelay bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.isDelay = isDelay
}

func (network *Network) SetLongReordering(isLongReordering bool) {
	network.mu.Lock()
	defer network.mu.Unlock()
	network.isLongReordering = isLongReordering
}



func (network *Network) GetTotal() int {
	return int(atomic.LoadInt32(&network.count))
}

func (network *Network) GetCount(address string) int {
	network.mu.Lock()
	defer network.mu.Unlock()
	host := network.hostMap[address]
	return host.getCount()
}

func (host *Host) getCount() int {
	host.mu.Lock()
	defer host.mu.Unlock()
	return host.count
}

func (network *Network) GetBytes() int64 {
	return atomic.LoadInt64(&network.bytes)
}



func (network *Network) Exit() {
	DropAndSet(network.exitCh)
}

