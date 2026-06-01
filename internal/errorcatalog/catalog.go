package errorcatalog

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"strconv"
)

type Entry struct {
	StatusCode int    `json:"-"`
	Code       string `json:"code"`
	Message    string `json:"message"`
}

//go:embed catalog.json
var catalogJSON []byte

var (
	byStatus = mustLoad(catalogJSON)
	byCode   = indexByCode(byStatus)
)

func ByStatus(statusCode int) Entry {
	if entry, ok := byStatus[statusCode]; ok {
		return entry
	}
	return Entry{StatusCode: statusCode, Code: "UNKNOWN_DEVICE_ERROR", Message: "Unknown device error"}
}

func ByCode(code string) Entry {
	if entry, ok := byCode[code]; ok {
		return entry
	}
	return byStatus[500]
}

func mustLoad(data []byte) map[int]Entry {
	var raw map[string]Entry
	if err := json.Unmarshal(data, &raw); err != nil {
		panic(fmt.Sprintf("invalid embedded error catalog: %v", err))
	}
	entries := make(map[int]Entry, len(raw))
	for key, entry := range raw {
		statusCode, err := strconv.Atoi(key)
		if err != nil || statusCode <= 0 || entry.Code == "" || entry.Message == "" {
			panic(fmt.Sprintf("invalid embedded error catalog entry %q", key))
		}
		entry.StatusCode = statusCode
		entries[statusCode] = entry
	}
	if _, ok := entries[200]; !ok {
		panic("invalid embedded error catalog: missing status 200")
	}
	return entries
}

func indexByCode(entries map[int]Entry) map[string]Entry {
	result := make(map[string]Entry, len(entries))
	for _, entry := range entries {
		if _, exists := result[entry.Code]; exists {
			panic(fmt.Sprintf("invalid embedded error catalog: duplicate code %q", entry.Code))
		}
		result[entry.Code] = entry
	}
	return result
}
