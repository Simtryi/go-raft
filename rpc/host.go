package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"log"
	"reflect"
	"strings"
	"sync"
)

//	host in the network can be either a client or a server
type Host struct {
	mu      		sync.Mutex
	Address			string
	serverEnable	bool				//	server or not
	serviceMap		map[string]*service	//	service.name => service
	reqCh			chan *request		//	copy of Network.reqCh
	exitCh			chan struct{}		//	copy of Network.exitCh
	count			int 				//	incoming RPCs
}

//	create host
func (network *Network) MakeHost(address string) *Host {
	network.mu.Lock()
	defer network.mu.Unlock()

	if _, dup := network.hostMap[address]; dup {
		log.Fatal("[Network] host already exist: ", address)
	}

	host := &Host{
		Address: 	address,
		serviceMap: make(map[string]*service),
		reqCh:   	network.reqCh,
		exitCh:  	network.exitCh,
	}

	network.hostMap[address] = host

	return host
}

//	start server
func (host *Host) Listen() {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.serverEnable = true
}

//	close server
func (host *Host) Close() {
	host.mu.Lock()
	defer host.mu.Unlock()
	host.serverEnable = false
}



//	synchronous invoke
func (host *Host)  Call(serverAddress string, serviceMethod string, args, reply interface{}) bool {
	req := new(request)
	req.clientAddress = host.Address
	req.serverAddress = serverAddress
	req.serviceMethod = serviceMethod
	req.resCh = make(chan *response)

	//	encode the argument
	writer := new(bytes.Buffer)
	enc := gob.NewEncoder(writer)
	err := enc.Encode(args)
	if err != nil {
		log.Fatal("[Client] encode error: ", err.Error())
	}
	req.args = writer.Bytes()

	select {
	case host.reqCh <- req:	//	send the request
		DPrintf("[Client] client(%s) send request: %v\n", host.Address, *req)
	case <- host.exitCh:
		DPrintf("[Client] exit client(%s)\n", host.Address)
		return false
	}

	//	wait for the response
	res := <- req.resCh
	if res.success {
		//	decode the reply
		reader := bytes.NewBuffer(res.reply)
		dec := gob.NewDecoder(reader)
		if err := dec.Decode(reply); err != nil {
			log.Fatal("[Client] decode error: ", err.Error())
		}
		return true
	}

	return false
}



//	register service
func (host *Host) Register(rcvr interface{}) {
	host.mu.Lock()
	defer host.mu.Unlock()

	s := makeService(rcvr)
	if _, dup := host.serviceMap[s.name]; dup {
		log.Fatal("[Server] service already register: ", s.name)
	}

	host.serviceMap[s.name] = s
	DPrintf("[Server] register service: %s\n", s.name)
}

//	handle request
func (host *Host) dispatch(req *request) *response {
	host.mu.Lock()
	defer host.mu.Unlock()

	host.count++

	s, m, err := host.find(req.serviceMethod)
	if err != nil {
		DPrintf(err.Error())
		return &response{false, nil}
	}

	//	decode the argument
	argType := m.Type.In(1)
	argv := reflect.New(argType)
	reader := bytes.NewBuffer(req.args)
	dec := gob.NewDecoder(reader)
	dec.Decode(argv.Interface())

	//	allocate space for the reply
	replyType := m.Type.In(2).Elem()
	replyv := reflect.New(replyType)

	//	call the method
	s.call(m, argv.Elem(), replyv)

	//	encode the reply
	writer := new(bytes.Buffer)
	enc := gob.NewEncoder(writer)
	err = enc.EncodeValue(replyv)
	if err != nil {
		log.Fatal("[Server] encode error: ", err.Error())
	}

	return &response{true, writer.Bytes()}
}

//	find service
func (host *Host) find(serviceMethod string) (s *service, m reflect.Method, err error) {
	dotIndex := strings.LastIndex(serviceMethod, ".")
	if dotIndex < 0 {
		err = errors.New("[Server] service/method request ill-formed: " + serviceMethod)
		return
	}

	serviceName, methodName := serviceMethod[:dotIndex], serviceMethod[dotIndex + 1:]
	s = host.serviceMap[serviceName]
	if s == nil {
		err = errors.New("[Server] can not find service: " + serviceName)
		return
	}

	m, ok := s.methodMap[methodName]
	if !ok {
		err = errors.New("[Server] can not find method: " + methodName)
		return
	}

	return
}