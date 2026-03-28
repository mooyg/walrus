package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/mooyg/walrus"
)

var version = "dev"

func main() {
	port := flag.Int("port", 9092, "gRPC listen port")
	dataDir := flag.String("data-dir", "./data", "base directory for commit log storage")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println("walrus", version)
		return
	}

	srv, err := walrus.NewServer(walrus.Config{
		Port:     *port,
		DataDir:  *dataDir,
		LogLevel: *logLevel,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create server: %v\n", err)
		os.Exit(1)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		srv.Stop()
	}()

	if err := srv.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "server error: %v\n", err)
		os.Exit(1)
	}
}
