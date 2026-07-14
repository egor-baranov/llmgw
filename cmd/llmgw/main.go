package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"llmgw/app"
)

func main() {
	if err := run(); err != nil {
		log.Printf("llmgw: %v", err)
		os.Exit(1)
	}
}

func run() error {
	configPath := flag.String("config", "config/config.example.yaml", "path to config file")
	printOpenAPI := flag.Bool("print-openapi", false, "print generated OpenAPI YAML and exit")
	flag.Parse()

	if *printOpenAPI {
		body, err := app.OpenAPIYAML(*configPath)
		if err != nil {
			return fmt.Errorf("generate openapi: %w", err)
		}
		if _, err := os.Stdout.Write(body); err != nil {
			return fmt.Errorf("write openapi: %w", err)
		}
		return nil
	}

	service, err := app.New(*configPath)
	if err != nil {
		return fmt.Errorf("init app: %w", err)
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- service.Serve()
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	defer signal.Stop(signals)
	for {
		select {
		case err := <-serveErr:
			serveFailure := err
			if serveFailure == nil {
				serveFailure = errors.New("server stopped unexpectedly")
			}
			return errors.Join(fmt.Errorf("serve: %w", serveFailure), shutdown(service))
		case sig := <-signals:
			if sig == syscall.SIGHUP {
				if err := service.Reload(); err != nil {
					log.Printf("reload: %v", err)
				}
				continue
			}
			shutdownErr := shutdown(service)
			if serveErrValue := <-serveErr; serveErrValue != nil {
				return errors.Join(fmt.Errorf("serve during shutdown: %w", serveErrValue), shutdownErr)
			}
			return shutdownErr
		}
	}
}

func shutdown(service *app.Service) error {
	ctx, cancel := context.WithTimeout(context.Background(), service.ShutdownTimeout())
	defer cancel()
	if err := service.Shutdown(ctx); err != nil {
		return fmt.Errorf("shutdown: %w", err)
	}
	return nil
}
