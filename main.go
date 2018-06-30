package main

import (
	"flag"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/golang/glog"
	"golang.org/x/net/context"
)

var (
	AwsRegion string
	port      string
	ds        *DataSource
)

func init() {
	flag.StringVar(&port, "port", "8080", "server port")
	flag.Parse()
	ds = InitDataSource()
}

func main() {
	glog.Infof("listening on %s", port)

	ctx, cancel := context.WithCancel(context.Background())

	go ds.run(&ctx)
	go gracefulServerShutdown(&cancel)

	router := NewRouter()
	router.Handle("open", connected)
	router.Handle("stream_data", streamData)
	http.Handle("/", router)
	http.ListenAndServe(":"+port, nil)
}

func gracefulServerShutdown(cancel *context.CancelFunc) {
	var gracefulStop = make(chan os.Signal)
	signal.Notify(gracefulStop, syscall.SIGTERM)
	signal.Notify(gracefulStop, syscall.SIGINT)
	for {
		select {
		case sig := <-gracefulStop:
			glog.Infof("caught sig: %+v", sig)
			(*cancel)()
			return
		default:
		}
	}
}
