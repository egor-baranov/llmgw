package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"llmgw/app"
)

func main() {
	configPath := flag.String("config", "config/config.example.yaml", "path to config file")
	printOpenAPI := flag.Bool("print-openapi", false, "print generated OpenAPI YAML and exit")
	flag.Parse()

	if *printOpenAPI {
		body, err := app.OpenAPIYAML(*configPath)
		if err != nil {
			log.Fatalf("generate openapi: %v", err)
		}
		if _, err := os.Stdout.Write(body); err != nil {
			log.Fatalf("write openapi: %v", err)
		}
		return
	}

	service, err := app.New(*configPath)
	if err != nil {
		log.Fatalf("init app: %v", err)
	}
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), service.ShutdownTimeout())
		defer cancel()
		_ = service.Shutdown(ctx)
	}()

	reload := make(chan os.Signal, 1)
	signal.Notify(reload, syscall.SIGHUP)
	go func() {
		for range reload {
			_ = service.Reload()
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-stop
		ctx, cancel := context.WithTimeout(context.Background(), service.ShutdownTimeout())
		defer cancel()
		_ = service.Shutdown(ctx)
	}()

	if err := service.Serve(); err != nil {
		log.Fatalf("serve: %v", err)
	}
}
