.PHONY: bench clean

bench:
	go test -bench . -cpuprofile cpu.out -blockprofile blocks.out -benchtime 5s

clean:
	rm -f *.out *.log

