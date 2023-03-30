package elastic_wrapper

import (
	"errors"
	"net/http"
)

var (
	ErrNotFound = NewResponseStatusError(http.StatusNotFound)
	ErrConflict = NewResponseStatusError(http.StatusConflict)
)

func NewResponseStatusError(code int) error {
	if successCode(code) {
		return nil
	}
	return ResponseStatusError(code)
}

type ResponseStatusError int

func (e ResponseStatusError) Error() string {
	return http.StatusText(int(e))
}

func IsResponseStatusError(err error, code int) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, ResponseStatusError(code))
}
