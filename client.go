package main

import (
	"context"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
	"github.com/satori/go.uuid"
)

const (
	CLIENT_OUT_MESSAGE_BUFFER = 50
	CLIENT_IN_MESSAGE_BUFFER  = 50
)

type MessageToClient struct {
	Name string      `json:"Name"`
	Data interface{} `json:"data"`
}

type FindHandler func(string) (Handler, bool)

type Client struct {
	send         chan MessageToClient
	socket       *websocket.Conn
	findHandler  FindHandler
	ctx          *context.Context
	cancel       context.CancelFunc
	Id           uuid.UUID
	Subsciptions map[Stream]chan []byte
	contextMap   map[Stream]*context.Context
	cancelMap    map[Stream]context.CancelFunc
}

func (c *Client) String() string {
	return string(c.Id[:len(c.Id)])
}

func (client *Client) Write() {
	for msg := range client.send {
		client.Infof("%#v", msg)
		if err := client.socket.WriteJSON(msg); err != nil {
			client.Errorf("%v", err)
			break
		}
	}
	client.socket.Close()
}

func (client *Client) Read() {
	var message MessageToClient
	for {
		if err := client.socket.ReadJSON(&message); err != nil {
			client.Errorf("%v", err)
			break
		}
		if handler, found := client.findHandler(message.Name); found {
			handler(client, message.Data)
		}
	}
	client.socket.Close()
}

func (client *Client) Close() {
	client.cancel()
	for stream, _ := range client.Subsciptions {
		client.Unsubscribe(stream)
	}
	close(client.send)
}

func NewClient(socket *websocket.Conn, findHandler FindHandler) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	uid := uuid.Must(uuid.NewV4())
	return &Client{
		send:         make(chan MessageToClient, CLIENT_OUT_MESSAGE_BUFFER),
		socket:       socket,
		findHandler:  findHandler,
		ctx:          &ctx,
		cancel:       cancel,
		Id:           uid,
		Subsciptions: make(map[Stream]chan []byte),
		contextMap:   make(map[Stream]*context.Context),
		cancelMap:    make(map[Stream]context.CancelFunc),
	}
}

func (c *Client) Subscribe(stream Stream) {
	channel := make(chan []byte, CLIENT_IN_MESSAGE_BUFFER)
	c.Subsciptions[stream] = channel
	ctx, cancel := context.WithCancel(context.Background())
	c.contextMap[stream] = &ctx
	c.cancelMap[stream] = cancel
	var sub subscriber
	sub = c
	ds.subChan <- SubscriptionMessage{
		stream:     stream,
		subscriber: &sub,
	}
}

func (c *Client) Unsubscribe(stream Stream) {
	delete(c.contextMap, stream)
	c.cancelMap[stream]()
	delete(c.cancelMap, stream)
	delete(c.Subsciptions, stream)
	var sub subscriber
	sub = c
	ds.unsubChan <- SubscriptionMessage{
		stream:     stream,
		subscriber: &sub,
	}
}

type Logger interface {
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

func (c *Client) Infof(format string, args ...interface{}) {
	glog.Infof(withId(format), c.Id, args)
}
func (c *Client) Warningf(format string, args ...interface{}) {
	glog.Warningf(withId(format), c.Id, args)
}
func (c *Client) Errorf(format string, args ...interface{}) {
	glog.Errorf(withId(format), c.Id, args)
}
func (c *Client) Fatalf(format string, args ...interface{}) {
	glog.Fatalf(withId(format), c.Id, args)
}

func withId(format string) string {
	return "[%v] " + format
}
