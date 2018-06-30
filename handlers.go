package main

import (
	"encoding/json"
	"strings"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
	"github.com/golang/glog"
	"github.com/mitchellh/mapstructure"
)

type StreamReq struct {
	Stream string
	Region string
}

type ListOfStreams struct {
	Streams []StreamImpl `json:"streams"`
}

type DataPoint struct {
	X string `json:"x"`
	Y string `json:"y"`
}

func connected(client *Client, data interface{}) {
	client.Infof("connected")
	client.send <- MessageToClient{
		Name: "connection_open",
		Data: nil,
	}
}

func subscribe(client *Client, data interface{}) {
	var req StreamReq
	err := mapstructure.Decode(data, &req)
	if err != nil {
		client.send <- MessageToClient{"error", err.Error()}
		return
	}
	client.Infof("Subscribing to %v", req.Stream)
	s := StreamImpl{
		Name:   req.Stream,
		Region: RegionName(req.Region),
	}
	client.Subscribe(&s)
	sendRecordsToClient(client, &s)
}

func unsubscribe(client *Client, data interface{}) {
	var req StreamReq
	err := mapstructure.Decode(data, &req)
	if err != nil {
		client.send <- MessageToClient{"error", err.Error()}
		return
	}
	client.Infof("Unsubscribing to %v", req.Stream)
	client.Unsubscribe(&StreamImpl{
		Name:   req.Stream,
		Region: RegionName(req.Region),
	})
}

func listStreams(client *Client, data interface{}) {
	streams := getAllStreams()
	client.send <- MessageToClient{
		Name: "list_of_streams",
		Data: ListOfStreams{
			Streams: streams,
		},
	}
}

func getAllStreams() []StreamImpl {
	streams := make([]StreamImpl, 0)
	for k, _ := range ds.kinesisMap {
		s := listStreamsFor(k)
		streams = append(streams, s...)
	}
	return streams
}

func listStreamsFor(region RegionName) []StreamImpl {
	ksis := (*kinesis.Kinesis)(ds.kinesisMap[region])
	hasMoreStreams := true
	streams := make([]StreamImpl, 0)
	for hasMoreStreams {
		resp, err := ksis.ListStreams()
		if err != nil {
			glog.Errorf("Error while retrieving the list of streams for Region : %s", string(region))
		}
		if resp == nil {
			break
		}
		for _, s := range resp.StreamNames {
			streams = append(streams, StreamImpl{
				Region: region,
				Name:   s,
			})
		}

		hasMoreStreams = resp.HasMoreStreams
	}
	return streams
}

func streamData(client *Client, data interface{}) {
	//client.Infof("streaming data")
	//// data interface would have the Name of the stream from the client
	//// start streaming data from that stream
	//var req StreamReq
	//err := mapstructure.Decode(data, &req)
	//if err != nil {
	//	client.send <- MessageToClient{"error", err.Error()}
	//	return
	//}
	//ksis := initKinesisClient(client)
	//streamDescription := waitUntilStreamActive(client, ksis, req.Stream)
	//sctx := &streamContext{
	//	ctx:          client.ctx,
	//	recordStream: make(chan []byte, 5000),
	//}
	//for _, shard := range streamDescription.Shards {
	//	reader := NewStreamReader(ksis, req.Stream, shard.ShardId)
	//	go reader.StreamRecords(sctx)
	//}
	//sendRecordsToClient(client, sctx)
}

func sendRecordsToClient(client *Client, stream Stream) {
	for {
		select {
		case record := <-client.Subsciptions[stream]:
			dp := decodeDataPoint(client, record)
			client.send <- MessageToClient{
				Name: "update_data",
				Data: dp,
			}
		case <-(*client.contextMap[stream]).Done():
			return
		default:

		}
	}
}

func decodeDataPoint(client *Client, data []byte) *DataPoint {
	var dp DataPoint
	err := json.Unmarshal(data, &dp)
	if err != nil {
		client.Errorf("%v", err)
	}
	return &dp
}

func initKinesisClient(client *Client) *kinesis.Kinesis {
	awsRegion := aws.Regions[strings.ToLower(AwsRegion)]
	auth, err := aws.EnvAuth()
	if err != nil {
		client.Errorf("Unable to authenticate with AWS %v\n", err)
	}
	return kinesis.New(auth, awsRegion)
}
