package main

import (
	"context"
	"testing"
)

func TestPubSubSingleSub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go ds.run(&ctx)
	client := testSubscriber().(*Client)
	stream := MockStream("DUMMY")
	client.Subscribe(stream)
	resultChan := make(chan string, 1)
	go poll(client.Subsciptions[stream], &ctx, resultChan)
	ds.recordStream <- RecordMessage{
		stream: stream,
		data:   []byte("Hello"),
	}
	result := <-resultChan
	if result == "FAILURE" {
		t.Fatal()
	}
	cancel()
}

func TestPubSubMultipleSub(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	go ds.run(&ctx)
	stream := MockStream("DUMMY")

	client1 := testSubscriber().(*Client)
	client1.Subscribe(stream)
	resultChan1 := make(chan string, 1)
	go poll(client1.Subsciptions[stream], &ctx, resultChan1)

	client2 := testSubscriber().(*Client)
	client2.Subscribe(stream)
	resultChan2 := make(chan string, 1)
	go poll(client2.Subsciptions[stream], &ctx, resultChan2)

	ds.recordStream <- RecordMessage{
		stream: stream,
		data:   []byte("Hello"),
	}

	result1 := <-resultChan1
	if result1 == "FAILURE" {
		t.Fatal()
	}
	result2 := <-resultChan2
	if result2 == "FAILURE" {
		t.Fatal()
	}

	client1.Unsubscribe(stream)
	go poll(client2.Subsciptions[stream], &ctx, resultChan2)

	ds.recordStream <- RecordMessage{
		stream: stream,
		data:   []byte("Hello"),
	}

	result2 = <-resultChan2
	if result2 == "FAILURE" {
		t.Fatal()
	}
	cancel()
	close(resultChan1)
	close(resultChan2)
}

func poll(data chan []byte, ctx *context.Context, resultChan chan string) {
	for {
		select {
		case d := <-data:
			message := string(d[:])
			if message != "Hello" {
				resultChan <- "FAILURE"
			} else {
				resultChan <- "SUCCESS"
			}
			return
		case <-(*ctx).Done():
			return
		default:

		}
	}
}
