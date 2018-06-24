package main

import (
	"flag"
	"net/http"
	"os"

	"github.com/golang/glog"
)

var (
	AwsRegion string
	port      string
)

func init() {
	AwsRegion = os.Getenv("AWS_REGION")
	flag.StringVar(&port, "port", "8080", "server port")
	flag.Parse()
}

func main() {
	glog.Infof("listening on %s", port)
	router := NewRouter()
	router.Handle("open", connected)
	router.Handle("stream_data", streamData)

	http.Handle("/", router)
	http.ListenAndServe(":"+port, nil)
}
