.PHONY: test test-astored test-all clean clean-astored clean-all install image

test:
	go test

test-astored:
	$(MAKE) -C astored test

test-all: test test-astored

clean: 
	rm -rf *.dat
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
