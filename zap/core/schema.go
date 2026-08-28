package core

import (
	"encoding/json"
	"reflect"
	"strings"
	"time"
)

// Schema describes every record zap puts on the wire, as JSON Schema draft
// 2020-12. It is REFLECTED from the Go types rather than written by hand: a
// schema maintained by hand is a second truth that drifts the first time a
// field is renamed, and `TestSchemaIsCurrent` fails when schema.json lags.
func Schema() map[string]any {
	models := []any{
		Account{}, Contact{}, SendRequest{}, SendResult{}, PairEvent{},
		Event{}, Session{}, Group{}, GroupMember{}, GroupRequest{},
		SyncRequest{}, SyncResult{}, Call{},
	}
	defs := map[string]any{}
	for _, m := range models {
		t := reflect.TypeOf(m)
		defs[t.Name()] = schemaOf(t)
	}
	// the event catalogue belongs in the schema: a consumer should not have to
	// read the source to learn which types can show up on /tail
	if event, ok := defs["Event"].(map[string]any); ok {
		if props, ok := event["properties"].(map[string]any); ok {
			props["type"] = map[string]any{"type": "string", "enum": Events.All()}
		}
	}
	return map[string]any{
		"$schema":     "https://json-schema.org/draft/2020-12/schema",
		"$id":         "https://github.com/biliboss/zap/schema.json",
		"title":       "zap",
		"description": "Records zap emits over the unix socket, one per line as ndjson.",
		"$defs":       defs,
	}
}

func schemaOf(t reflect.Type) map[string]any {
	props := map[string]any{}
	var required []string

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name, opts, ok := jsonName(field)
		if !ok {
			continue
		}
		props[name] = typeOf(field.Type)
		if !strings.Contains(opts, "omitempty") {
			required = append(required, name)
		}
	}
	out := map[string]any{"type": "object", "properties": props, "additionalProperties": false}
	if len(required) > 0 {
		out["required"] = required
	}
	return out
}

func jsonName(field reflect.StructField) (name, opts string, ok bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", "", false
	}
	name, opts, _ = strings.Cut(tag, ",")
	if name == "" {
		name = field.Name
	}
	return name, opts, true
}

func typeOf(t reflect.Type) map[string]any {
	if t == reflect.TypeOf(time.Time{}) {
		return map[string]any{"type": "string", "format": "date-time"}
	}
	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int64, reflect.Int32:
		return map[string]any{"type": "integer"}
	case reflect.Slice:
		return map[string]any{"type": "array", "items": typeOf(t.Elem())}
	case reflect.Ptr:
		return typeOf(t.Elem())
	case reflect.Struct:
		return map[string]any{"$ref": "#/$defs/" + t.Name()}
	default:
		return map[string]any{}
	}
}

func SchemaJSON() ([]byte, error) {
	raw, err := json.MarshalIndent(Schema(), "", "  ")
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}
