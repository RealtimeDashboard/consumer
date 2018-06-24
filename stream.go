package main

import (
	"time"

	"github.com/AdRoll/goamz/kinesis"
)

type Reader struct {
	kinesis.Kinesis
	logger       Logger
	streamName   string
	shardId      string
	iteratorType kinesis.ShardIteratorType
}

var recordLimit = 100

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
				r.logger.Errorf("%v", err)
				time.Sleep(time.Duration(1 * time.Second))
				continue
			}
			if len(records.Records) > 0 {
				for _, record := range records.Records {
					sctx.recordStream <- record.Data
				}
			} else if records.NextShardIterator == "" || shardIterator == records.NextShardIterator || err != nil {
				r.logger.Errorf("Unable to iterate over records %v", err)
				break
			}
			shardIterator = records.NextShardIterator
		}
	}
}

func (r *Reader) shardIterator() string {
	shardIteratorRes, err := r.Kinesis.GetShardIterator(r.shardId, r.streamName, r.iteratorType, "")
	r.logger.Infof("%v", err)
	return shardIteratorRes.ShardIterator
}

func NewStreamReader(logger Logger, ksis *kinesis.Kinesis, streamName string, shardId string) *Reader {
	return &Reader{
		logger:       logger,
		Kinesis:      *ksis,
		streamName:   streamName,
		shardId:      shardId,
		iteratorType: kinesis.ShardIteratorLatest,
	}
}
