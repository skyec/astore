.PHONY: test clean clean-astored clean-all install image

test:
	go test ./...

clean: 
	rm -rf *.dat *.out *.test
	rm -rf build

clean-astored:
	$(MAKE) -C astored clean

clean-all: clean clean-astored

install:
	godep restore
	go install ./...

image:
	docker build -t astored-image .

run:
	docker run -d -P --name astored astored-image:latest 
