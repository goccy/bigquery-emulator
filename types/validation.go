package types

import (
	"strings"

	"github.com/go-playground/validator/v10"
)

func TypeValidation(fl validator.FieldLevel) bool {
	return Type(fl.Field().String()).ZetaSQLTypeKind().String() != ""
}

func ModeValidation(fl validator.FieldLevel) bool {
	mode := fl.Field().String()
	if mode == "" {
		return true
	}
	switch strings.ToLower(mode) {
	case strings.ToLower(string(NullableMode)):
		return true
	case strings.ToLower(string(RequiredMode)):
		return true
	case strings.ToLower(string(RepeatedMode)):
		return true
	}
	return false
}

func RegisterTypeValidation(v *validator.Validate) {
	v.RegisterValidation("type", TypeValidation)
	v.RegisterValidation("mode", ModeValidation)
}
