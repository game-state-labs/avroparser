# Avro Parser

A Go tool to decode Avro files produced by the Apache Pulsar S3 sink (`CloudStorageGenericRecordSink`).

## Overview

This tool reads Avro files containing `PulsarRawMessage` records (with a `message` field of type `bytes`) and extracts the embedded JSON payloads into a consolidated JSON output file.

## Installation

```bash
# Install dependencies
go mod tidy

# Build the binary
go build -o avroparser main.go
```

## Usage

```bash
# Using go run
go run main.go -input <avro_file> [-output <output_dir>] [-pretty=true|false]

# Using the built binary
./avroparser -input <avro_file> [-output <output_dir>] [-pretty=true|false]
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `-input` | (required) | Path to the input Avro file |
| `-output` | `output` | Output directory for JSON files |
| `-pretty` | `true` | Pretty print JSON output with indentation |

### Examples

```bash
# Basic usage - decode an Avro file
go run main.go -input input/1280.1.-1.avro

# Specify custom output directory
go run main.go -input input/1280.1.-1.avro -output /tmp/decoded

# Compact JSON output (no indentation)
go run main.go -input input/1280.1.-1.avro -pretty=false
```

## Pulsar Sink Configuration

This tool is designed to work with Avro files produced by a Pulsar S3 sink with the following configuration:

```json
{
  "className": "org.apache.pulsar.io.jcloud.sink.CloudStorageGenericRecordSink",
  "configs": {
    "formatType": "avro",
    "avroCodec": "snappy"
  }
}
```

## Output

The tool outputs a JSON array containing all decoded messages from the Avro file. The output file is named after the input file with a `.json` extension.

