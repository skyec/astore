FROM golang:latest 
RUN go get github.com/tools/godep

RUN mkdir -p /go/src/github.com/skyec/astore
ADD . /go/src/github.com/skyec/astore
WORKDIR /go/src/github.com/skyec/astore 

RUN make install
EXPOSE 9898

CMD ["/go/bin/astored"]
