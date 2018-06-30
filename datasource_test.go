package main

import (
	"testing"

	"github.com/golang/glog"
	"github.com/satori/go.uuid"
)

func TestSubscribersIndexEmpty(t *testing.T) {
	subs := newSubscribers()
	s := testSubscriber()
	if subs.index(&s) >= 0 {
		t.Fatal("testSubscribers should empty")
	}
}

func TestSubscribersIndexSingleSubs(t *testing.T) {
	subs := newSubscribers()
	s := testSubscriber()
	subs.add(&s)
	if subs.index(&s) < 0 {
		t.Fatal("subscriber not found")
	}
	s1 := testSubscriber()
	if subs.index(&s1) >= 0 {
		t.Fatal("subscribers should not be found")
	}
}

func TestSubscribersRemoveSingleSubs(t *testing.T) {
	subs := newSubscribers()
	s := testSubscriber()
	subs.add(&s)

	s1 := testSubscriber()
	if subs.remove(&s1) {
		t.Fatal("subscribers should not be removed")
	}

	if !subs.remove(&s) {
		t.Fatal("subscriber not removed")
	}

	s2 := testSubscriber()
	if subs.remove(&s2) {
		t.Fatal("subscribers should not be removed")
	}
}

func TestSubscriptionSubscribe(t *testing.T) {
	var ds subscriptions
	ds = make(map[Stream]*subscribers)
	s := testSubscriber()
	stream := MockStream("Dummy")
	if ds.unsubscribe(stream, &s) {
		t.Fatal("Fail")
	}
	ds.subscribe(stream, &s)
	if !ds.unsubscribe(stream, &s) {
		t.Fatal("Fail")
	}
}

func TestSubscriptionMultipleSubscribe(t *testing.T) {
	var ds subscriptions
	ds = make(map[Stream]*subscribers)
	s1 := testSubscriber()
	s2 := testSubscriber()
	stream := MockStream("1")
	ds.subscribe(stream, &s1)
	ds.subscribe(stream, &s2)
	s3 := testSubscriber()
	if ds.unsubscribe(stream, &s3) {
		t.Fatal("Fail")
	}
}

func testSubscriber() subscriber {
	id := uuid.Must(uuid.NewV4())
	c := Client{
		Id:           id,
		Subsciptions: make(map[Stream]chan []byte),
	}
	return &c
}

type MockStream string

func (ms MockStream) start() {
	glog.Infof("streaming started")
}
func (ms MockStream) String() string {
	return string(ms)
}
