package errorcatalog

import "testing"

func TestCatalogLookups(t *testing.T) {
	success := ByStatus(200)
	if success.Code != "OK" || success.Message != "Success" {
		t.Fatalf("unexpected success entry: %+v", success)
	}

	invalidArgument := ByStatus(1001)
	if invalidArgument.Code != "INVALID_ARGUMENT" {
		t.Fatalf("unexpected device error entry: %+v", invalidArgument)
	}

	unknown := ByStatus(1999)
	if unknown.StatusCode != 1999 || unknown.Code != "UNKNOWN_DEVICE_ERROR" {
		t.Fatalf("unexpected unknown device error entry: %+v", unknown)
	}

	internal := ByCode("DOES_NOT_EXIST")
	if internal.StatusCode != 500 || internal.Code != "INTERNAL_ERROR" {
		t.Fatalf("unexpected fallback entry: %+v", internal)
	}
}
