package main

import (
	"fmt"
	"flag"
	"net/http"
	"os"

	"github.com/config"
	"github.com/logger"
	"github.com/ctxutil"

	"github.com/tus/tusd"
	//pprof采集
	_"net/http/pprof"

	"server/util"
)

type HttpConfig struct {
	Addrs string
}

func main() {
	var configPath string
	flag.StringVar(&configPath, "c", "", "--config=xxx/uploadserver.conf")
	flag.Parse()
	if configPath == "" {
		usage()
		return
	}
	cfg, err := config.New(configPath)

	if err != nil {
		printAndDie(err)
		return
	}
	if err = logger.NewLoggerWithConfig(cfg); err != nil {
		printAndDie(err)
		return
	}
	logger.RegisterContextFormat(ctxutil.TraceString)
	logger.Info(logger.TAIHETagUploadStart, "start to init")
	// Create a new FileStore instance which is responsible for
	// storing the uploaded file on disk in the specified directory.
	// This path _must_ exist before tusd will store uploads in it.
	// If you want to save them on a different medium, for example
	// a remote FTP server, you can implement your own storage backend
	// by implementing the tusd.DataStore interface.
	store, err := util.GetUploadStore(cfg)
	if err != nil {
		printAndDie(err)
		return
	}

	// A storage backend for tusd may consist of multiple different parts which
	// handle upload creation, locking, termination and so on. The composer is a
	// place where all those separated pieces are joined together. In this example
	// we only use the file store but you may plug in multiple.
	composer := tusd.NewStoreComposer()
	store.UseIn(composer)

	// Create a new HTTP handler for the tusd server by providing a configuration.
	// The StoreComposer property must be set to allow the handler to function.
	tusdConfig, err := util.GetTusdConfig(cfg)
	if err != nil {
		printAndDie(err)
		return
	}
	tusdConfig.StoreComposer = composer

	handler, err := tusd.NewHandler(tusdConfig)
	if err != nil {
		panic(fmt.Errorf("Unable to create handler: %s", err))
	}

	// Right now, nothing has happened since we need to start the HTTP server on
	// our own. In the end, tusd will start listening on and accept request at
	// http://localhost:8080/files
	http.Handle(tusdConfig.BasePath, http.StripPrefix(tusdConfig.BasePath, handler))
	httpConfig, err := util.GetHttpConfig(cfg)
	if err != nil {
		printAndDie(err)
		return
	}

	err = http.ListenAndServe(httpConfig.Addrs, nil)
	if err != nil {
		panic(fmt.Errorf("Unable to listen: %s", err))
	}
}

///////////////////////////tools////////////////////////
func usage() {
	fmt.Fprintf(os.Stdout, "please run \"%s --help \" and get help info\n", os.Args[0])
	return
}

func printAndDie(err error) {
	fmt.Fprintf(os.Stderr, "init failed err:%s", err)
	os.Exit(-1)
	return
}