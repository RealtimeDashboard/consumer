package main

import (
	"context"
	"strings"
	"time"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
	"github.com/golang/glog"
)

const (
	SUBSCRIBE_CHAN_BUFFER   = 10
	UNSUBSCRIBE_CHAN_BUFFER = 10
	RECORD_STREAM_BUFFER    = 50
)

//------------ Stream ------------

type Stream interface {
	start()
	String() string
}

type StreamImpl struct {
	Name   string     `json:"name"`
	Region RegionName `json:"region"`
}

func (s *StreamImpl) start() {
	ksis := ds.kinesisMap[s.Region]
	streamDescription := ksis.waitUntilStreamActive(s)
	ctx, cancel := context.WithCancel(context.Background())
	sctx := &streamContext{
		ctx:          &ctx,
		recordStream: ds.recordStream,
	}
	ds.cancelStream[s] = cancel
	for _, shard := range streamDescription.Shards {
		reader := NewStreamReader((*kinesis.Kinesis)(ksis), s, shard.ShardId)
		go reader.StreamRecords(sctx)
	}
}

func (s *StreamImpl) String() string {
	return s.Name
}

//------------ subscriber ------------
type subscriber interface {
	accept(message *RecordMessage)
	equals(other *Client) bool
}

func (c *Client) equals(other *Client) bool {
	return c.Id == other.Id
}

func (c *Client) accept(message *RecordMessage) {
	c.Subsciptions[message.stream] <- message.data
}

//------------ subscribers ------------
type subscribers struct {
	subs []*subscriber
}

func newSubscribers() *subscribers {
	return &subscribers{
		subs: make([]*subscriber, 0),
	}
}

func (s *subscribers) add(sub *subscriber) {
	s.subs = append(s.subs, sub)
}

// remove subscriber
func (s *subscribers) remove(sub *subscriber) bool {
	subs := &s.subs
	if index := s.index(sub); index >= 0 {
		(*subs)[len(*subs)-1], (*subs)[index] = (*subs)[index], (*subs)[len(*subs)-1]
		s := (*subs)[:len((*subs))-1]
		subs = &s
		return true
	}
	return false
}

// find the index of the subscriber in the list, -1 if does not contains
func (s *subscribers) index(sub *subscriber) int {
	for i := 0; i < len(s.subs); i++ {
		if (*s.subs[i]).equals((*sub).(*Client)) {
			return i
		}
	}
	return -1
}

// ------------ Subscription ------------

type subscriptions map[Stream]*subscribers

func (s *subscriptions) subscribe(stream Stream, subscriber *subscriber) {
	if (*s)[stream] == nil {
		stream.start()
		(*s)[stream] = newSubscribers()
	}
	if (*s)[stream].index(subscriber) >= 0 {
		glog.Errorf("subscriber %x is already subscribed to stream %s", subscriber, stream)
		return
	}
	(*s)[stream].add(subscriber)
	glog.Infof("subscriber %x added for stream %s, count : %v", subscriber, stream, len((*s)[stream].subs))
}

func (s *subscriptions) unsubscribe(stream Stream, subscriber *subscriber) bool {
	if (*s)[stream] == nil {
		glog.Errorf("Unsubscribing %x from stream %v which does not have any subscribers", subscriber, stream)
		return false
	}
	if !(*s)[stream].remove(subscriber) {
		glog.Errorf("%x is not subscribed to stream %v", subscriber, stream)
		return false
	}
	if len((*s)[stream].subs) == 0 {
		// no subscribers left so delete the stream
		delete((*s), stream)
		// cancel streaming from the stream
		ds.cancelStream[stream]()
	}
	glog.Infof("subscriber %x removed for stream %s, count : %v", subscriber, stream, len((*s)[stream].subs))
	return true
}

//------------ Data Source ------------

type SubscriptionMessage struct {
	stream     Stream
	subscriber *subscriber
}

type RegionName string

type DataSource struct {
	kinesisMap    map[RegionName]*KinesisClient
	subscriptions subscriptions
	subChan       chan SubscriptionMessage
	unsubChan     chan SubscriptionMessage
	cancelStream  map[Stream]context.CancelFunc
	recordStream  chan RecordMessage
}

func (ds *DataSource) run(ctx *context.Context) {
	glog.Info("Starting the kinesis data source")
	for {
		select {
		case m := <-ds.subChan:
			ds.subscriptions.subscribe(m.stream, m.subscriber)
		case m := <-ds.unsubChan:
			ds.subscriptions.unsubscribe(m.stream, m.subscriber)
		case rm := <-ds.recordStream:
			ds.publish(&rm)
		case <-(*ctx).Done():
			glog.Info("Stopping the kinesis data source")
			return
		}
	}
}

func (ds *DataSource) publish(message *RecordMessage) {
	subscribers := ds.subscriptions[message.stream]
	for _, s := range subscribers.subs {
		(*s).accept(message)
	}
}

func InitDataSource() *DataSource {
	kmap := make(map[RegionName]*KinesisClient)
	for k, v := range aws.Regions {
		if strings.Contains(k, "gov") {
			continue
		}
		ksis := newKinesisClient(&v)
		kmap[RegionName(k)] = ksis
	}
	return &DataSource{
		kinesisMap:    kmap,
		subscriptions: make(map[Stream]*subscribers),
		subChan:       make(chan SubscriptionMessage, SUBSCRIBE_CHAN_BUFFER),
		unsubChan:     make(chan SubscriptionMessage, UNSUBSCRIBE_CHAN_BUFFER),
		recordStream:  make(chan RecordMessage, RECORD_STREAM_BUFFER),
		cancelStream:  make(map[Stream]context.CancelFunc),
	}
}

//------------ Kinesis Client ------------

type KinesisClient kinesis.Kinesis

func newKinesisClient(awsRegion *aws.Region) *KinesisClient {
	auth, err := aws.EnvAuth()
	if err != nil {
		glog.Errorf("Unable to authenticate with AWS %v\n", err)
	}
	client := KinesisClient(*kinesis.New(auth, *awsRegion))
	return &client
}

func (client *KinesisClient) waitUntilStreamActive(streamName Stream) *kinesis.StreamDescription {
	ksis := (*kinesis.Kinesis)(client)
	streamDescription := &kinesis.StreamDescription{}
	for {
		streamDescription, _ = ksis.DescribeStream(streamName.String())
		if streamDescription.StreamStatus == kinesis.StreamStatusActive {
			break
		} else {
			glog.Infof("Stream status is %s\n", streamDescription.StreamStatus)
			time.Sleep(4 * time.Second)
		}
	}
	return streamDescription
}
