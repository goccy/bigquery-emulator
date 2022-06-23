package server

import (
	"context"

	"github.com/goccy/bigquery-emulator/internal/metadata"
)

type (
	serverKey  struct{}
	projectKey struct{}
	datasetKey struct{}
	jobKey     struct{}
	tableKey   struct{}
	modelKey   struct{}
	routineKey struct{}
)

func withServer(ctx context.Context, server *Server) context.Context {
	return context.WithValue(ctx, serverKey{}, server)
}

func serverFromContext(ctx context.Context) *Server {
	return ctx.Value(serverKey{}).(*Server)
}

func withProject(ctx context.Context, project *metadata.Project) context.Context {
	return context.WithValue(ctx, projectKey{}, project)
}

func projectFromContext(ctx context.Context) *metadata.Project {
	return ctx.Value(projectKey{}).(*metadata.Project)
}

func withDataset(ctx context.Context, dataset *metadata.Dataset) context.Context {
	return context.WithValue(ctx, datasetKey{}, dataset)
}

func datasetFromContext(ctx context.Context) *metadata.Dataset {
	return ctx.Value(datasetKey{}).(*metadata.Dataset)
}

func withJob(ctx context.Context, job *metadata.Job) context.Context {
	return context.WithValue(ctx, jobKey{}, job)
}

func jobFromContext(ctx context.Context) *metadata.Job {
	return ctx.Value(jobKey{}).(*metadata.Job)
}

func withTable(ctx context.Context, table *metadata.Table) context.Context {
	return context.WithValue(ctx, tableKey{}, table)
}

func tableFromContext(ctx context.Context) *metadata.Table {
	return ctx.Value(tableKey{}).(*metadata.Table)
}

func withModel(ctx context.Context, model *metadata.Model) context.Context {
	return context.WithValue(ctx, modelKey{}, model)
}

func modelFromContext(ctx context.Context) *metadata.Model {
	return ctx.Value(modelKey{}).(*metadata.Model)
}

func withRoutine(ctx context.Context, routine *metadata.Routine) context.Context {
	return context.WithValue(ctx, routineKey{}, routine)
}

func routineFromContext(ctx context.Context) *metadata.Routine {
	return ctx.Value(routineKey{}).(*metadata.Routine)
}
