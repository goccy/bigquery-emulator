package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/goccy/bigquery-emulator/server"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/jessevdk/go-flags"
)

type option struct {
	Project      string           `description:"specify the project name" long:"project"`
	Dataset      string           `description:"specify the dataset name" long:"dataset"`
	Port         uint16           `description:"specify the port number" long:"port" default:"9050"`
	LogLevel     server.LogLevel  `description:"specify the log level (debug/info/warn/error)" long:"log-level" default:"error"`
	LogFormat    server.LogFormat `description:"sepcify the log format (console/json)" long:"log-format" default:"console"`
	Database     string           `description:"specify the database file if required. if not specified, it will be on memory" long:"database"`
	DataFromYAML string           `description:"specify the path to the YAML file that contains the initial data" long:"data-from-yaml"`
	Version      bool             `description:"print version" long:"version" short:"v"`
}

type exitCode int

const (
	exitOK    exitCode = 0
	exitError exitCode = 1
)

var (
	version  string
	revision string
)

func main() {
	os.Exit(int(run()))
}

func run() exitCode {
	args, opt, err := parseOpt()
	if err != nil {
		flagsErr, ok := err.(*flags.Error)
		if !ok {
			fmt.Fprintf(os.Stderr, "[bigquery-emulator] unknown parsed option error: %[1]T %[1]v\n", err)
			return exitError
		}
		if flagsErr.Type == flags.ErrHelp {
			return exitOK
		}
		return exitError
	}
	if err := runServer(args, opt); err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitError
	}
	return exitOK
}

func parseOpt() ([]string, option, error) {
	var opt option
	parser := flags.NewParser(&opt, flags.Default)
	args, err := parser.Parse()
	return args, opt, err
}

func runServer(args []string, opt option) error {
	if opt.Version {
		fmt.Fprintf(os.Stdout, "version: %s (%s)\n", version, revision)
		return nil
	}
	if opt.Project == "" {
		return fmt.Errorf("the required flag --project was not specified")
	}
	var db server.Storage
	if opt.Database == "" {
		db = server.TempStorage
	} else {
		db = server.Storage(fmt.Sprintf("file:%s?cache=shared", opt.Database))
	}
	project := types.NewProject(opt.Project)
	if opt.Dataset != "" {
		project.Datasets = append(project.Datasets, types.NewDataset(opt.Dataset))
	}
	bqServer, err := server.New(db)
	if err != nil {
		return err
	}
	if err := bqServer.SetProject(project.ID); err != nil {
		return err
	}
	if err := bqServer.Load(server.StructSource(project)); err != nil {
		return err
	}
	if err := bqServer.SetLogLevel(opt.LogLevel); err != nil {
		return err
	}
	if err := bqServer.SetLogFormat(opt.LogFormat); err != nil {
		return err
	}
	if opt.DataFromYAML != "" {
		if err := bqServer.Load(server.YAMLSource(opt.DataFromYAML)); err != nil {
			return err
		}
	}

	ctx := context.Background()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	go func() {
		select {
		case s := <-interrupt:
			fmt.Fprintf(os.Stdout, "[bigquery-emulator] receive %s. shutdown gracefully\n", s)
			if err := bqServer.Stop(ctx); err != nil {
				fmt.Fprintf(os.Stderr, "[bigquery-emulator] failed to stop: %v\n", err)
			}
		}
	}()

	done := make(chan error)
	go func() {
		addr := fmt.Sprintf("0.0.0.0:%d", opt.Port)
		fmt.Fprintf(os.Stdout, "[bigquery-emulator] listening at %s\n", addr)
		done <- bqServer.Serve(ctx, addr)
	}()

	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}

	return nil
}
