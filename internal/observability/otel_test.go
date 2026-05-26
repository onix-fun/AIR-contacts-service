package observability

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/trace"
)

func resetInitFactories(t *testing.T) {
	t.Helper()
	originalMerge := mergeResources
	originalTrace := newTraceExporter
	originalMetric := newMetricExporter
	originalRuntime := startRuntimeCollection
	t.Cleanup(func() {
		mergeResources = originalMerge
		newTraceExporter = originalTrace
		newMetricExporter = originalMetric
		startRuntimeCollection = originalRuntime
	})
}

func TestInitDisabled(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	shutdown, err := Init(context.Background())
	if err != nil {
		t.Fatalf("disabled init: %v", err)
	}
	if err := shutdown(context.Background()); err != nil {
		t.Fatalf("disabled shutdown: %v", err)
	}
}

func TestInitFailuresAndSuccess(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "false")
	expected := errors.New("failed")

	t.Run("resource", func(t *testing.T) {
		resetInitFactories(t)
		mergeResources = func(*resource.Resource, *resource.Resource) (*resource.Resource, error) {
			return nil, expected
		}
		if _, err := Init(context.Background()); !errors.Is(err, expected) {
			t.Fatalf("expected resource error, got %v", err)
		}
	})

	t.Run("trace exporter", func(t *testing.T) {
		resetInitFactories(t)
		newTraceExporter = func(context.Context, ...otlptracehttp.Option) (*otlptrace.Exporter, error) {
			return nil, expected
		}
		if _, err := Init(context.Background()); !errors.Is(err, expected) {
			t.Fatalf("expected trace exporter error, got %v", err)
		}
	})

	t.Run("metric exporter", func(t *testing.T) {
		resetInitFactories(t)
		newMetricExporter = func(context.Context, ...otlpmetrichttp.Option) (*otlpmetrichttp.Exporter, error) {
			return nil, expected
		}
		if _, err := Init(context.Background()); !errors.Is(err, expected) {
			t.Fatalf("expected metric exporter error, got %v", err)
		}
	})

	t.Run("runtime", func(t *testing.T) {
		resetInitFactories(t)
		startRuntimeCollection = func(...otelruntime.Option) error { return expected }
		if _, err := Init(context.Background()); !errors.Is(err, expected) {
			t.Fatalf("expected runtime error, got %v", err)
		}
	})

	t.Run("success", func(t *testing.T) {
		resetInitFactories(t)
		startRuntimeCollection = func(...otelruntime.Option) error { return nil }
		shutdown, err := Init(context.Background())
		if err != nil {
			t.Fatalf("init: %v", err)
		}
		_ = shutdown(context.Background())
	})
}

func TestCorrelationMiddleware(t *testing.T) {
	var received string
	next := http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		received = CorrelationID(r.Context())
	})
	handler := CorrelationMiddleware(next)

	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(CorrelationIDHeader, "  correlation-id  ")
	handler.ServeHTTP(recorder, request)
	if received != "correlation-id" || recorder.Header().Get(CorrelationIDHeader) != received {
		t.Fatalf("expected preserved correlation ID, got %q", received)
	}

	recorder = httptest.NewRecorder()
	request = httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set(CorrelationIDHeader, "bad id")
	handler.ServeHTTP(recorder, request)
	if received == "bad id" || !IsValidCorrelationID(received) {
		t.Fatalf("expected generated correlation ID, got %q", received)
	}
}

func TestCorrelationHelpersAndTraceFields(t *testing.T) {
	ctx := context.Background()
	if CorrelationID(WithCorrelationID(ctx, "bad id")) != "" {
		t.Fatal("invalid correlation ID must not be stored")
	}
	ctx = WithCorrelationID(ctx, "correlation")
	if CorrelationID(ctx) != "correlation" {
		t.Fatal("expected stored correlation ID")
	}
	cases := []string{"", strings.Repeat("x", 129), "has space", "line\nbreak", "\u00e9"}
	for _, value := range cases {
		if IsValidCorrelationID(value) {
			t.Fatalf("expected invalid correlation ID %q", value)
		}
	}
	if !IsValidCorrelationID("valid-123") {
		t.Fatal("expected valid correlation ID")
	}
	if fields := TraceFields(ctx); !strings.Contains(fields, "trace_id= span_id= correlation_id=correlation") {
		t.Fatalf("unexpected trace fields %q", fields)
	}
	spanContext := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID: trace.TraceID{1},
		SpanID:  trace.SpanID{2},
	})
	fields := TraceFields(trace.ContextWithSpanContext(ctx, spanContext))
	if !strings.Contains(fields, "trace_id=01000000000000000000000000000000") ||
		!strings.Contains(fields, "span_id=0200000000000000") {
		t.Fatalf("unexpected populated trace fields %q", fields)
	}
}

func TestResourceAttributesAndEnv(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "region=west,invalid,=empty,key=, owner = contacts ")
	attrs := resourceAttributes()
	if len(attrs) != 3 {
		t.Fatalf("unexpected resource attributes: %v", attrs)
	}
	if env("UNSET_VALUE", "fallback") != "fallback" {
		t.Fatal("expected environment fallback")
	}
	t.Setenv("SET_VALUE", " configured ")
	if env("SET_VALUE", "fallback") != "configured" {
		t.Fatal("expected trimmed environment value")
	}
	t.Setenv("OTEL_SERVICE_NAME", "custom-service")
	if len(resourceAttributes()) == 0 {
		t.Fatal("expected configured service attribute")
	}
}
