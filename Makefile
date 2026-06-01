.PHONY: help build run test clean proto docker-build docker-up docker-down

help:
	@echo "Device WebSocket Service - Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  make proto        - Generate code from proto files"
	@echo "  make build        - Build binary"
	@echo "  make run          - Run service locally"
	@echo "  make test         - Run tests"
	@echo "  make clean        - Clean build artifacts"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build - Build Docker image"
	@echo "  make docker-up    - Start services with docker-compose"
	@echo "  make docker-down  - Stop services"
	@echo "  make docker-logs  - View docker-compose logs"
	@echo ""
	@echo "Dependencies:"
	@echo "  make deps         - Download dependencies"
	@echo "  make tidy         - Tidy dependencies"

proto:
	@echo "Generating proto code..."
	protoc --go_out=. --go-grpc_out=. api/proto/access.proto
	mkdir -p internal/proto
	mv github.com/onix-air/contacts/api/proto/access/*.go internal/proto/
	rm -rf github.com

deps:
	@echo "Downloading dependencies..."
	go mod download

tidy:
	@echo "Tidying dependencies..."
	go mod tidy

build: tidy
	@echo "Building device-ws..."
	go build -o device-ws ./cmd/device-ws
	@echo "✓ Build complete: ./device-ws"

run: build
	@echo "Running service..."
	./device-ws

test:
	@echo "Running tests..."
	go test -v ./...

clean:
	@echo "Cleaning..."
	rm -f device-ws
	go clean
	@echo "✓ Clean complete"

docker-build:
	@echo "Building Docker image..."
	docker build -t device-ws:latest .
	@echo "✓ Docker image built"

docker-up:
	@echo "Starting services with docker-compose..."
	docker-compose up -d
	@echo "✓ Services started"
	@echo "  Redis:      localhost:6379"
	@echo "  Mosquitto:  localhost:1883"
	@echo "  Device WS:  localhost:8080"

docker-down:
	@echo "Stopping services..."
	docker-compose down
	@echo "✓ Services stopped"

docker-logs:
	docker-compose logs -f

fmt:
	@echo "Formatting code..."
	go fmt ./...
	@echo "✓ Format complete"

lint:
	@echo "Running linter..."
	golangci-lint run ./...

install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
	@echo "✓ Tools installed"
