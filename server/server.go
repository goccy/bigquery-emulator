package server

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/goccy/bigquery-emulator/internal/contentdata"
	"github.com/goccy/bigquery-emulator/internal/metadata"
	"github.com/gorilla/mux"
)

type Server struct {
	Handler     http.Handler
	storage     Storage
	db          *sql.DB
	metaRepo    *metadata.Repository
	contentRepo *contentdata.Repository
	fileCleanup func() error
}

func New(storage Storage) (*Server, error) {
	server := &Server{storage: storage}
	if storage == TempStorage {
		f, err := os.CreateTemp("", "")
		if err != nil {
			return nil, fmt.Errorf("failed to create temporary file: %w", err)
		}
		storage = Storage(fmt.Sprintf("file:%s?cache=shared", f.Name()))
		server.storage = storage
		server.fileCleanup = func() error {
			return os.Remove(f.Name())
		}
	}
	db, err := sql.Open("zetasqlite", string(storage))
	if err != nil {
		return nil, err
	}
	server.db = db
	metaRepo, err := metadata.NewRepository(db)
	if err != nil {
		return nil, err
	}
	server.metaRepo = metaRepo
	server.contentRepo = contentdata.NewRepository(db)

	r := mux.NewRouter()
	for _, handler := range handlers {
		r.Handle(handler.Path, handler.Handler).Methods(handler.HTTPMethod)
	}
	r.PathPrefix("/").Handler(&defaultHandler{})
	r.Use(recoveryMiddleware())
	r.Use(withServerMiddleware(server))
	r.Use(withProjectMiddleware())
	r.Use(withDatasetMiddleware())
	r.Use(withJobMiddleware())
	r.Use(withTableMiddleware())
	r.Use(withModelMiddleware())
	r.Use(withRoutineMiddleware())
	server.Handler = r
	return server, nil
}

func (s *Server) Close() error {
	defer func() {
		if s.fileCleanup != nil {
			if err := s.fileCleanup(); err != nil {
				log.Printf("failed to cleanup file: %s", err.Error())
			}
		}
	}()
	if err := s.db.Close(); err != nil {
		log.Printf("failed to close database: %s", err.Error())
		return err
	}
	return nil
}

func (s *Server) SetProject(id string) error {
	ctx := context.Background()
	tx, err := s.metaRepo.Begin(ctx)
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}
	defer tx.Commit()
	if err := s.metaRepo.AddProjectIfNotExists(
		ctx,
		tx,
		metadata.NewProject(s.metaRepo, id, nil, nil),
	); err != nil {
		return err
	}
	return nil
}

func (s *Server) Load(sources ...Source) error {
	for _, source := range sources {
		if err := source(s); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) Serve(ctx context.Context, addr string) error {
	srv := &http.Server{
		Handler:      s.Handler,
		Addr:         addr,
		WriteTimeout: 15 * time.Second,
		ReadTimeout:  15 * time.Second,
	}
	return srv.ListenAndServe()
}

func (s *Server) TestServer() *httptest.Server {
	return httptest.NewServer(s.Handler)
}
