package rpc

import (
	"go/ast"
	"log"
	"reflect"
)

//	rpc service, i.e. struct
type service struct {
	rcvr    	reflect.Value				//	struct value
	typ     	reflect.Type				//	struct type
	name    	string						//	struct name
	methodMap 	map[string]reflect.Method	//	mName => reflect.Method
}

//	create service
func makeService(rcvr interface{}) *service {
	s := new(service)
	s.rcvr = reflect.ValueOf(rcvr)
	s.typ = reflect.TypeOf(rcvr)

	s.name = reflect.Indirect(s.rcvr).Type().Name()
	if !ast.IsExported(s.name) {
		log.Fatalf("[Server] %s is not a valid service name", s.name)
	}

	s.methodMap = make(map[string]reflect.Method)
	s.registerMethods()

	return s
}

//	register service's methods
func (s *service) registerMethods() {
	for i := 0; i < s.typ.NumMethod(); i++ {
		method := s.typ.Method(i)
		mType := method.Type
		mName := method.Name

		// input parameters is 3
		if mType.NumIn() != 3 {
			continue
		}

		argType, replyType := mType.In(1), mType.In(2)
		if !isExportedOrBuiltinType(argType) || !isExportedOrBuiltinType(replyType) {
			continue
		}

		if replyType.Kind() != reflect.Ptr {
			continue
		}

		s.methodMap[mName] = method
		DPrintf("[Server] register method %s.%s\n", s.name, mName)
	}
}

//	call the method
func (s *service) call(m reflect.Method, argv reflect.Value, replyv reflect.Value) {
	f := m.Func
	f.Call([]reflect.Value{s.rcvr, argv, replyv})
}

func isExportedOrBuiltinType(t reflect.Type) bool {
	return ast.IsExported(t.Name()) || t.PkgPath() == ""
}
