package scripts

import (
	"crypto/sha1"
	"encoding/hex"
	"io"
)

// gen script name by script; name keep update with script
var UpdateTypesScriptName = func() string {
	h := sha1.New()
	_, _ = io.WriteString(h, UpdateTypesScript)
	return hex.EncodeToString(h.Sum(nil))
}()

type UpdateType string

const (
	UpdateType_undefined UpdateType = ""
	UpdateType_set       UpdateType = "set"
	UpdateType_incr      UpdateType = "incr"
	UpdateType_push      UpdateType = "push"
)

type UpdateField struct {
	Tp    UpdateType `json:"tp"`
	Name  string     `json:"name"`
	Value any        `json:"value"`
}

const UpdateTypesScript = `
	for (item in params.fields) {
		int tp = item['tp'];
		if (tp == 'set') {
			ctx._source[item['name']] = item['value'];
		} else if (tp == 'incr') {
			ctx._source[item['name']] = (ctx._source[item['name']] == null? item['value']: ctx._source[item['name']]+item['value']);
		} else if (tp == 'push') {
			if (ctx._source[item['name']] == null) {
				ctx._source[item['name']] = item['value'];
			} else {
				for (v in item['value']) {
					ctx._source[item['name']].add(v);
				}
			}
		}
	}
`
