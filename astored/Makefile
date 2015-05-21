.PHONY: test clean bench

astored: *.go
	go build

clean:
	rm -f astored
test:
	go test

bench: astored
	go test -bench .
