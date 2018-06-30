package main

import (
	"context"
	"time"

	"github.com/AdRoll/goamz/kinesis"
	"github.com/golang/glog"
)

type Reader struct {
	kinesis.Kinesis
	stream       Stream
	shardId      string
	iteratorType kinesis.ShardIteratorType
}

var recordLimit = 100

type streamContext struct {
	ctx          *context.Context
	recordStream chan RecordMessage
}

type RecordMessage struct {
	stream Stream
	data   []byte
}

func (r *Reader) StreamRecords(sctx *streamContext) {
	shardIterator := r.shardIterator()
	for {
		select {
		case <-(*sctx.ctx).Done():
			return
		default:
			records, err := r.Kinesis.GetRecords(shardIterator, recordLimit)
			time.Sleep(time.Duration(200 * time.Millisecond))
			if err != nil {
				glog.Errorf("%v", err)
				time.Sleep(time.Duration(1 * time.Second))
				continue
			}
			if len(records.Records) > 0 {
				for _, record := range records.Records {
					sctx.recordStream <- RecordMessage{
						data:   record.Data,
						stream: r.stream,
					}
				}
			} else if records.NextShardIterator == "" || shardIterator == records.NextShardIterator || err != nil {
				glog.Errorf("Unable to iterate over records %v", err)
				break
			}
			shardIterator = records.NextShardIterator
		}
	}
}

func (r *Reader) shardIterator() string {
	shardIteratorRes, err := r.Kinesis.GetShardIterator(r.shardId, r.stream.String(), r.iteratorType, "")
	glog.Infof("%v", err)
	return shardIteratorRes.ShardIterator
}

func NewStreamReader(ksis *kinesis.Kinesis, stream Stream, shardId string) *Reader {
	return &Reader{
		Kinesis:      *ksis,
		stream:       stream,
		shardId:      shardId,
		iteratorType: kinesis.ShardIteratorLatest,
	}
}
