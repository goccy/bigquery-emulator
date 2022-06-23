package types

import (
	"github.com/go-playground/validator/v10"
)

func TypeValidation(fl validator.FieldLevel) bool {
	return Type(fl.Field().String()).ZetaSQLTypeKind().String() != ""
}

func RegisterTypeValidation(v *validator.Validate) {
	v.RegisterValidation("type", TypeValidation)
}
