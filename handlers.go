package main

import (
	"encoding/json"
	"strings"

	"github.com/AdRoll/goamz/aws"
	"github.com/AdRoll/goamz/kinesis"
	"github.com/mitchellh/mapstructure"
)

type StreamReq struct {
	Stream string
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
	//ds.subscribe(Stream(req.Stream), &client.subscriber)

}

func streamData(client *Client, data interface{}) {
	//client.Infof("streaming data")
	//// data interface would have the name of the stream from the client
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

func sendRecordsToClient(client *Client, sctx *streamContext) {
	//for {
	//	select {
	//	case record := <-sctx.recordStream:
	//		dp := decodeDataPoint(client, record)
	//		client.send <- MessageToClient{
	//			Name: "update_data",
	//			Data: dp,
	//		}
	//	case <-(*sctx.ctx).Done():
	//		return
	//	}
	//}
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
