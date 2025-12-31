package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/linkedin/goavro/v2"
)

func main() {
	inputFile := flag.String("input", "", "Input Avro file path")
	outputDir := flag.String("output", "output", "Output directory for JSON files")
	prettyPrint := flag.Bool("pretty", true, "Pretty print JSON output")
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Usage: avroparser -input <avro_file> [-output <output_dir>] [-pretty=true|false]")
		os.Exit(1)
	}

	// Read the Avro file
	data, err := os.ReadFile(*inputFile)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Create OCF reader
	ocfReader, err := goavro.NewOCFReader(bytes.NewReader(data))
	if err != nil {
		fmt.Printf("Error creating OCF reader: %v\n", err)
		os.Exit(1)
	}

	// Create output directory if it doesn't exist
	if err := os.MkdirAll(*outputDir, 0755); err != nil {
		fmt.Printf("Error creating output directory: %v\n", err)
		os.Exit(1)
	}

	// Collect all messages
	var allMessages []json.RawMessage
	messageCount := 0

	for ocfReader.Scan() {
		record, err := ocfReader.Read()
		if err != nil {
			fmt.Printf("Error reading record: %v\n", err)
			continue
		}

		// The record is a map with "message" field containing bytes
		recordMap, ok := record.(map[string]interface{})
		if !ok {
			fmt.Printf("Record is not a map: %T\n", record)
			continue
		}

		messageBytes, ok := recordMap["message"].([]byte)
		if !ok {
			fmt.Printf("Message field is not bytes: %T\n", recordMap["message"])
			continue
		}

		// The message bytes contain JSON - validate and add to collection
		var jsonData json.RawMessage
		if err := json.Unmarshal(messageBytes, &jsonData); err != nil {
			fmt.Printf("Warning: Message %d is not valid JSON, saving as raw bytes\n", messageCount)
			// Save as raw string if not valid JSON
			jsonData = json.RawMessage(fmt.Sprintf("%q", string(messageBytes)))
		}

		allMessages = append(allMessages, jsonData)
		messageCount++
	}

	if err := ocfReader.Err(); err != nil {
		fmt.Printf("Error during OCF iteration: %v\n", err)
	}

	fmt.Printf("Decoded %d messages from Avro file\n", messageCount)

	// Write all messages to a single JSON file
	baseName := filepath.Base(*inputFile)
	baseName = baseName[:len(baseName)-len(filepath.Ext(baseName))]
	outputFile := filepath.Join(*outputDir, baseName+".json")

	var outputData []byte
	if *prettyPrint {
		outputData, err = json.MarshalIndent(allMessages, "", "  ")
	} else {
		outputData, err = json.Marshal(allMessages)
	}

	if err != nil {
		fmt.Printf("Error marshaling JSON: %v\n", err)
		os.Exit(1)
	}

	if err := os.WriteFile(outputFile, outputData, 0644); err != nil {
		fmt.Printf("Error writing output file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Output written to: %s\n", outputFile)
}
