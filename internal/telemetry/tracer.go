package telemetry

import (
	"context"
	"io"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	globalTracer trace.Tracer
	tp           *sdktrace.TracerProvider
)

func InitTracer(ctx context.Context, serviceName string) (*sdktrace.TracerProvider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceNameKey.String(serviceName)),
	)
	if err != nil {
		return nil, err
	}
	var exporter sdktrace.SpanExporter
	e, err := stdouttrace.New(
		stdouttrace.WithWriter(io.Discard),
	)
	if err != nil {
		return nil, err
	}
	exporter = e
	var otelOps []otlptracegrpc.Option
	if collector := os.Getenv("OTEL_COLLECTOR"); collector != "" {
		conn, err := grpc.NewClient(collector,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			return nil, err
		}
		otelOps = append(otelOps, otlptracegrpc.WithGRPCConn(conn))
		exporter, err = otlptracegrpc.New(ctx, otelOps...)
		if err != nil {
			return nil, err
		}
	}

	tp = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)

	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	globalTracer = otel.Tracer(serviceName)
	return tp, nil
}

func Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return globalTracer.Start(ctx, spanName, opts...)
}

func GetTraceProvider() *sdktrace.TracerProvider {
	return tp
}
