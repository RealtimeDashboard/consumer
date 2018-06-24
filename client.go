package main

import (
	"context"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
	"github.com/satori/go.uuid"
)

type MessageToClient struct {
	Name string      `json:"name"`
	Data interface{} `json:"data"`
}

type FindHandler func(string) (Handler, bool)

type Client struct {
	id          uuid.UUID
	send        chan MessageToClient
	socket      *websocket.Conn
	findHandler FindHandler
	ctx         *context.Context
	cancel      context.CancelFunc
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
	close(client.send)
}

func NewClient(socket *websocket.Conn, findHandler FindHandler) *Client {
	ctx, cancel := context.WithCancel(context.Background())
	uid := uuid.Must(uuid.NewV4())
	return &Client{
		id:          uid,
		send:        make(chan MessageToClient),
		socket:      socket,
		findHandler: findHandler,
		ctx:         &ctx,
		cancel:      cancel,
	}
}

type Logger interface {
	Infof(format string, args ...interface{})
	Warningf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
	Fatalf(format string, args ...interface{})
}

func (c *Client) Infof(format string, args ...interface{}) {
	glog.Infof(withId(format), c.id, args)
}
func (c *Client) Warningf(format string, args ...interface{}) {
	glog.Warningf(withId(format), c.id, args)
}
func (c *Client) Errorf(format string, args ...interface{}) {
	glog.Errorf(withId(format), c.id, args)
}
func (c *Client) Fatalf(format string, args ...interface{}) {
	glog.Fatalf(withId(format), c.id, args)
}

func withId(format string) string {
	return "[%v] " + format
}
