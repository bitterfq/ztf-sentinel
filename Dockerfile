FROM golang:1.25

WORKDIR /app

# copy dependencies first
COPY go.mod go.sum ./
RUN go mod download

# copy source code
COPY cmd/ cmd/
COPY internal/ internal/

# create logs dir
RUN mkdir -p logs

# Build & run the binary
RUN go build -o sentinel ./cmd/sentinel
CMD [ "./sentinel" ]