package main

import (
	"fmt"
	"time"

	"github.com/AdRoll/goamz/kinesis"
	"github.com/golang/glog"
)

type Reader struct {
	kinesis.Kinesis
	streamName   string
	shardId      string
	iteratorType kinesis.ShardIteratorType
}

var recordLimit = 100

func (r *Reader) StreamRecords1() {
	shardIterator := r.shardIterator()
	for {
		select {
		default:
			records, err := r.Kinesis.GetRecords(shardIterator, recordLimit)
			time.Sleep(time.Duration(200*time.Millisecond))
			if err != nil {
				fmt.Errorf("%v \n", err)
				return
			}
			if len(records.Records) > 0 {
				for _, record := range records.Records {
					//sctx.recordStream <- record.Data
					dp := decodeDataPoint(record.Data)
					fmt.Printf("%s : %s\n", dp.X, dp.Y)
				}
			} else if records.NextShardIterator == "" || shardIterator == records.NextShardIterator || err != nil {
				fmt.Errorf("Unable to iterate over records %v\n", err)
				break
			}
			shardIterator = records.NextShardIterator
		}
	}
}

func (r *Reader) StreamRecords(sctx *streamContext) {
	shardIterator := r.shardIterator()
	for {
		select {
		case <-(*sctx.ctx).Done():
			return
		default:
			records, err := r.Kinesis.GetRecords(shardIterator, recordLimit)
			time.Sleep(time.Duration(200*time.Millisecond))
			if err != nil {
				fmt.Errorf("%v \n", err)
				time.Sleep(time.Duration(1*time.Second))
				continue
			}
			if len(records.Records) > 0 {
				for _, record := range records.Records {
					sctx.recordStream <- record.Data
				}
			} else if records.NextShardIterator == "" || shardIterator == records.NextShardIterator || err != nil {
				glog.Errorf("Unable to iterate over records %v\n", err)
				break
			}
			shardIterator = records.NextShardIterator
		}
	}
}

func (r *Reader) shardIterator() string {
	shardIteratorRes, err := r.Kinesis.GetShardIterator(r.shardId, r.streamName, r.iteratorType, "")
	glog.Infof("%v \n", err)
	return shardIteratorRes.ShardIterator
}

func NewStreamReader(ksis *kinesis.Kinesis, streamName string, shardId string) *Reader {
	return &Reader{
		Kinesis:      *ksis,
		streamName:   streamName,
		shardId:      shardId,
		iteratorType: kinesis.ShardIteratorLatest,
	}
}
