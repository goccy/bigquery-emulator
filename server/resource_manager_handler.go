package server

import (
	"context"
	"net/http"

	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-json"
	"google.golang.org/api/cloudresourcemanager/v1"
)

const (
	projectAPIEndpoint = "/v1/projects"
)

// https://cloud.google.com/resource-manager/reference/rest/v1/projects/create
type projectsCreateHandler struct{}

type projectsCreateRequest struct {
	cloudResourceManagerProject *cloudresourcemanager.Project
	server                      *Server
}

func (h *projectsCreateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	server := serverFromContext(ctx)
	cloudResourceManagerProject := new(cloudresourcemanager.Project)
	if err := json.NewDecoder(r.Body).Decode(cloudResourceManagerProject); err != nil {
		errorResponse(ctx, w, errInvalid(err.Error()))
		return
	}
	res, err := h.Handle(ctx, &projectsCreateRequest{
		cloudResourceManagerProject: cloudResourceManagerProject,
		server:                      server,
	})
	if err != nil {
		errorResponse(ctx, w, errInternalError(err.Error()))
		return
	}
	encodeResponse(ctx, w, res)
}

func (h *projectsCreateHandler) Handle(ctx context.Context, r *projectsCreateRequest) (*cloudresourcemanager.Operation, error) {
	r.server.addProject(ctx, types.NewProject(r.cloudResourceManagerProject.ProjectId))
	return &cloudresourcemanager.Operation{Done: true}, nil
}
