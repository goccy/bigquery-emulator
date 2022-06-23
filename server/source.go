package server

import (
	"bytes"
	"context"
	"errors"
	"os"

	"github.com/go-playground/validator/v10"
	"github.com/goccy/bigquery-emulator/types"
	"github.com/goccy/go-yaml"
)

type Source func(*Server) error

func YAMLSource(path string) Source {
	return func(s *Server) error {
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		validate := validator.New()
		types.RegisterTypeValidation(validate)
		dec := yaml.NewDecoder(
			bytes.NewBuffer(content),
			yaml.Validator(validate),
			yaml.Strict(),
		)
		var v struct {
			Projects []*types.Project `yaml:"projects" validate:"required"`
		}
		if err := dec.Decode(&v); err != nil {
			return errors.New(yaml.FormatError(err, false, true))
		}
		return s.addProjects(context.Background(), v.Projects)
	}
}

func StructSource(projects ...*types.Project) Source {
	return func(s *Server) error {
		return s.addProjects(context.Background(), projects)
	}
}
