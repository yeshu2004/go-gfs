build:
	@go build -o bin/storage
run: build
	@./bin/storage
test:
	@go test -v ./...