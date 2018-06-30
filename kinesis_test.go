package main

import (
	"testing"

	"github.com/golang/glog"
)

func TestListStreams(t *testing.T) {
	ds = InitDataSource()
	streams := getAllStreams()
	for _, s := range streams {
		glog.Infof("%s, %s", s.Name, s.Region)
	}
}
