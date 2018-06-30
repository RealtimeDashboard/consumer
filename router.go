package main

import (
	"fmt"
	"net/http"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	// todo: check origin, remove the func below
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Handler func(*Client, interface{})

type Router struct {
	rules map[string]Handler
}

func NewRouter() *Router {
	return &Router{
		rules: make(map[string]Handler),
	}
}

func (e *Router) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	glog.Info("serving now")
	socket, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		fmt.Fprint(w, err.Error())
		return
	}
	client := NewClient(socket, e.FindHandler)
	defer client.Close()
	go client.Write()
	client.Read()
}

func (r *Router) FindHandler(msgName string) (Handler, bool) {
	handler, found := r.rules[msgName]
	return handler, found
}

func (r *Router) Handle(msgName string, handler Handler) {
	r.rules[msgName] = handler
}
