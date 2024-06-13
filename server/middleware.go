package server

import (
	"compress/gzip"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"

	"github.com/gorilla/mux"
	"go.uber.org/zap"

	"github.com/goccy/bigquery-emulator/internal/logger"
)

func sequentialAccessMiddleware() func(http.Handler) http.Handler {
	var mu sync.Mutex
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			next.ServeHTTP(w, r)
		})
	}
}

func recoveryMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					ctx := logger.WithLogger(r.Context(), s.logger)
					errorResponse(ctx, w, errInternalError(fmt.Sprintf("%+v", err)))
					var frame int = 1
					for {
						_, file, line, ok := runtime.Caller(frame)
						if !ok {
							break
						}
						s.logger.Error(fmt.Sprintf("%d: %v:%d", frame, file, line))
						frame++
					}
					return
				}
			}()
			next.ServeHTTP(w, r)
		})
	}
}

func loggerMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			next.ServeHTTP(w, r.WithContext(logger.WithLogger(ctx, s.logger)))
		})
	}
}

func accessLogMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			logger.Logger(r.Context()).Info(
				fmt.Sprintf("%s %s", r.Method, r.URL.Path),
				zap.String("query", r.URL.RawQuery),
			)
			next.ServeHTTP(w, r)
		})
	}
}

const (
	contentEncoding  = "Content-Encoding"
	encodingTypeGzip = "gzip"
)

func decompressMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get(contentEncoding) != encodingTypeGzip {
				next.ServeHTTP(w, r)
				return
			}
			ctx := r.Context()
			reader, err := gzip.NewReader(r.Body)
			if err != nil {
				errorResponse(ctx, w, errInvalid(fmt.Sprintf("failed to decode gzip content: %s", err)))
				return
			}
			defer reader.Close()
			r.Body = reader
			next.ServeHTTP(w, r)
		})
	}
}

func withServerMiddleware(s *Server) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			next.ServeHTTP(
				w,
				r.WithContext(withServer(r.Context(), s)),
			)
		})
	}
}

func projectIDFromParams(params map[string]string) (string, bool) {
	projectID, exists := params["projectId"]
	if exists {
		return projectID, true
	}
	projectID, exists = params["projectsId"]
	return projectID, exists
}

func datasetIDFromParams(params map[string]string) (string, bool) {
	datasetID, exists := params["datasetId"]
	if exists {
		return datasetID, true
	}
	datasetID, exists = params["datasetsId"]
	return datasetID, exists
}

func jobIDFromParams(params map[string]string) (string, bool) {
	jobID, exists := params["jobId"]
	if exists {
		return jobID, true
	}
	jobID, exists = params["jobsId"]
	return jobID, exists
}

func tableIDFromParams(params map[string]string) (string, bool) {
	tableID, exists := params["tableId"]
	if exists {
		return tableID, true
	}
	tableID, exists = params["tablesId"]
	return tableID, exists
}

func modelIDFromParams(params map[string]string) (string, bool) {
	modelID, exists := params["modelId"]
	if exists {
		return modelID, true
	}
	modelID, exists = params["modelsId"]
	return modelID, exists
}

func routineIDFromParams(params map[string]string) (string, bool) {
	routineID, exists := params["routineId"]
	if exists {
		return routineID, true
	}
	routineID, exists = params["routinesId"]
	return routineID, exists
}

func withProjectMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			projectID, exists := projectIDFromParams(params)
			if exists {
				server := serverFromContext(ctx)
				project, err := server.metaRepo.FindProject(ctx, projectID)
				if err != nil {
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, err)
					return
				}
				if project == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("project %s is not found", projectID)))
					return
				}
				ctx = withProject(ctx, project)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withMethodOverrideMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			methodOverride := r.Header.Get("X-HTTP-Method-Override")
			if methodOverride == "" {
				next.ServeHTTP(w, r)
				return
			}
			r.Method = methodOverride
			next.ServeHTTP(w, r)
		})
	}
}

func withDatasetMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			datasetID, exists := datasetIDFromParams(params)
			if exists {
				project := projectFromContext(ctx)
				log.Printf("[mfudala] project %s", project.ID)
				dataset := project.Dataset(datasetID)
				if dataset == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("dataset %s is not found", datasetID)))
					return
				}
				ctx = withDataset(ctx, dataset)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withJobMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			jobID, exists := jobIDFromParams(params)
			if exists {
				project := projectFromContext(ctx)
				job := project.Job(jobID)
				if job == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("job %s is not found", jobID)))
					return
				}
				ctx = withJob(ctx, job)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withTableMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			tableID, exists := tableIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				table := dataset.Table(tableID)
				if table == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("table %s is not found", tableID)))
					return
				}
				ctx = withTable(ctx, table)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withModelMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			modelID, exists := modelIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				model := dataset.Model(modelID)
				if model == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("model %s is not found", modelID)))
					return
				}
				ctx = withModel(ctx, model)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}

func withRoutineMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ctx := r.Context()
			params := mux.Vars(r)
			routineID, exists := routineIDFromParams(params)
			if exists {
				dataset := datasetFromContext(ctx)
				routine := dataset.Routine(routineID)
				if routine == nil {
					errorResponse(ctx, w, errNotFound(fmt.Sprintf("routine %s is not found", routineID)))
					return
				}
				ctx = withRoutine(ctx, routine)
			}
			next.ServeHTTP(
				w,
				r.WithContext(ctx),
			)
		})
	}
}
