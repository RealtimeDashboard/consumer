package main

import (
	"flag"
	"fmt"
	"net/http"
)

var (
	//StreamName string
	AwsRegion string
	port      string
)

func init() {
	//flag.StringVar(&StreamName, "stream-name", "ChimeMetricStream", "your stream name")
	flag.StringVar(&AwsRegion, "aws-region", "us-west-2", "your AWS region")
	flag.StringVar(&port, "port", "8000", "server port")
	flag.Parse()
}

func main() {
	flag.Parse()

	fmt.Println("listening on " + port)
	router := NewRouter()
	router.Handle("open", connected)
	router.Handle("stream_data", streamData)

	http.Handle("/", router)
	http.ListenAndServe(":"+port, nil)
}
