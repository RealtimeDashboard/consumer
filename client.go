package main

import (
	"context"
	"fmt"

	"github.com/golang/glog"
	"github.com/gorilla/websocket"
)

type MessageToClient struct {
	Name string      `json:"name"`
	Data interface{} `json:"data"`
}

type FindHandler func(string) (Handler, bool)

type Client struct {
	send        chan MessageToClient
	socket      *websocket.Conn
	findHandler FindHandler
	ctx         *context.Context
	cancel      context.CancelFunc
}

func (client *Client) Write() {
	for msg := range client.send {
		glog.Infof("%#v\n", msg)
		if err := client.socket.WriteJSON(msg); err != nil {
			break
		}
	}
	client.socket.Close()
}

func (client *Client) Read() {
	var message MessageToClient
	for {
		if err := client.socket.ReadJSON(&message); err != nil {
			fmt.Printf("%v \n", err)
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
	return &Client{
		send:        make(chan MessageToClient),
		socket:      socket,
		findHandler: findHandler,
		ctx:         &ctx,
		cancel:      cancel,
	}
}
