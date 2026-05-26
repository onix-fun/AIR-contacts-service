package observability

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	otelruntime "go.opentelemetry.io/contrib/instrumentation/runtime"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const CorrelationIDHeader = "X-Correlation-Id"

type ShutdownFunc func(context.Context) error

type correlationIDContextKey struct{}

var (
	mergeResources         = resource.Merge
	newTraceExporter       = otlptracehttp.New
	newMetricExporter      = otlpmetrichttp.New
	startRuntimeCollection = otelruntime.Start
)

func Init(ctx context.Context) (ShutdownFunc, error) {
	if strings.EqualFold(os.Getenv("OTEL_SDK_DISABLED"), "true") {
		return func(context.Context) error { return nil }, nil
	}

	res, err := mergeResources(
		resource.Default(),
		resource.NewWithAttributes("", resourceAttributes()...),
	)
	if err != nil {
		return nil, err
	}

	traceExporter, err := newTraceExporter(ctx)
	if err != nil {
		return nil, err
	}

	metricExporter, err := newMetricExporter(ctx)
	if err != nil {
		_ = traceExporter.Shutdown(ctx)
		return nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.TraceIDRatioBased(1.0))),
	)
	meterProvider := metric.NewMeterProvider(
		metric.WithResource(res),
		metric.WithReader(metric.NewPeriodicReader(metricExporter, metric.WithInterval(15*time.Second))),
	)

	otel.SetTracerProvider(tracerProvider)
	otel.SetMeterProvider(meterProvider)
	if err := startRuntimeCollection(
		otelruntime.WithMeterProvider(meterProvider),
		otelruntime.WithMinimumReadMemStatsInterval(15*time.Second),
	); err != nil {
		_ = meterProvider.Shutdown(ctx)
		_ = tracerProvider.Shutdown(ctx)
		return nil, err
	}
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	return func(ctx context.Context) error {
		return errors.Join(
			meterProvider.Shutdown(ctx),
			tracerProvider.Shutdown(ctx),
		)
	}, nil
}

func CorrelationMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		correlationID := strings.TrimSpace(r.Header.Get(CorrelationIDHeader))
		if !IsValidCorrelationID(correlationID) {
			correlationID = uuid.NewString()
		}

		w.Header().Set(CorrelationIDHeader, correlationID)
		ctx := WithCorrelationID(r.Context(), correlationID)
		trace.SpanFromContext(ctx).SetAttributes(attribute.String("correlation.id", correlationID))

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func WithCorrelationID(ctx context.Context, correlationID string) context.Context {
	if !IsValidCorrelationID(correlationID) {
		return ctx
	}
	return context.WithValue(ctx, correlationIDContextKey{}, correlationID)
}

func CorrelationID(ctx context.Context) string {
	correlationID, _ := ctx.Value(correlationIDContextKey{}).(string)
	return correlationID
}

func IsValidCorrelationID(correlationID string) bool {
	if len(correlationID) == 0 || len(correlationID) > 128 {
		return false
	}
	for _, item := range correlationID {
		if item < 33 || item > 126 {
			return false
		}
	}
	return true
}

func TraceFields(ctx context.Context) string {
	spanContext := trace.SpanContextFromContext(ctx)
	correlationID := CorrelationID(ctx)
	if !spanContext.IsValid() {
		return fmt.Sprintf("trace_id= span_id= correlation_id=%s", correlationID)
	}
	return fmt.Sprintf(
		"trace_id=%s span_id=%s correlation_id=%s",
		spanContext.TraceID().String(),
		spanContext.SpanID().String(),
		correlationID,
	)
}

func resourceAttributes() []attribute.KeyValue {
	serviceName := env("OTEL_SERVICE_NAME", "contacts-device-ws")
	attrs := []attribute.KeyValue{
		attribute.String("service.name", serviceName),
	}

	for _, item := range strings.Split(os.Getenv("OTEL_RESOURCE_ATTRIBUTES"), ",") {
		key, value, ok := strings.Cut(item, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if key == "" || value == "" {
			continue
		}
		attrs = append(attrs, attribute.String(key, value))
	}

	return attrs
}

func env(name, fallback string) string {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback
	}
	return value
}
