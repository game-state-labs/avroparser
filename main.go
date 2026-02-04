package main

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/linkedin/goavro/v2"
)

// EventMessage represents the top-level message structure
type EventMessage struct {
	PlayerID    string       `json:"playerID"`
	GameID      string       `json:"gameID"`
	Country     string       `json:"country"`
	EventGroups []EventGroup `json:"eventGroups"`
	BatchID     string       `json:"batchID"`
	SDKVersion  string       `json:"sdkVersion"`
}

// EventGroup represents a group of events
type EventGroup struct {
	PlayerID    string  `json:"player_id"`
	SessionID   string  `json:"session_id"`
	DeviceID    string  `json:"device_id"`
	DeviceOS    string  `json:"device_os"`
	DeviceModel string  `json:"device_model"`
	AppVersion  string  `json:"app_version"`
	Events      []Event `json:"events"`
}

// Event represents a single event
type Event struct {
	ID        string          `json:"id"`
	EventName string          `json:"event_name"`
	Timestamp interface{}     `json:"timestamp"`
	SceneName string          `json:"scene_name"`
	Payload   json.RawMessage `json:"payload"`
}

func main() {
	inputDir := flag.String("input", "input", "Input directory containing Avro files")
	outputFile := flag.String("output", "output/all-events.json", "Output file path (.json or .csv)")
	prettyPrint := flag.Bool("pretty", true, "Pretty print JSON output (only for JSON format)")
	flag.Parse()

	// Scan input directory for all files
	inputFiles, err := scanInputDirectory(*inputDir)
	if err != nil {
		fmt.Printf("Error scanning input directory: %v\n", err)
		os.Exit(1)
	}

	if len(inputFiles) == 0 {
		fmt.Printf("No files found in input directory: %s\n", *inputDir)
		os.Exit(1)
	}

	fmt.Printf("Found %d files in %s\n", len(inputFiles), *inputDir)

	// Determine output format from extension
	outputExt := strings.ToLower(filepath.Ext(*outputFile))
	if outputExt != ".json" && outputExt != ".csv" {
		fmt.Println("Error: Output file must have .json or .csv extension")
		os.Exit(1)
	}

	// Create output directory if it doesn't exist
	outputDir := filepath.Dir(*outputFile)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Printf("Error creating output directory: %v\n", err)
			os.Exit(1)
		}
	}

	// Collect all messages from all input files
	var allMessages []json.RawMessage
	totalMessageCount := 0

	for _, inputFile := range inputFiles {
		fmt.Printf("Processing: %s\n", inputFile)

		// Read the Avro file
		data, err := os.ReadFile(inputFile)
		if err != nil {
			fmt.Printf("Error reading file %s: %v\n", inputFile, err)
			continue
		}

		// Create OCF reader
		ocfReader, err := goavro.NewOCFReader(bytes.NewReader(data))
		if err != nil {
			fmt.Printf("Error creating OCF reader for %s: %v\n", inputFile, err)
			continue
		}

		messageCount := 0
		for ocfReader.Scan() {
			record, err := ocfReader.Read()
			if err != nil {
				fmt.Printf("Error reading record from %s: %v\n", inputFile, err)
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
				fmt.Printf("Warning: Message %d in %s is not valid JSON, saving as raw bytes\n", messageCount, inputFile)
				// Save as raw string if not valid JSON
				jsonData = json.RawMessage(fmt.Sprintf("%q", string(messageBytes)))
			}

			allMessages = append(allMessages, jsonData)
			messageCount++
		}

		if err := ocfReader.Err(); err != nil {
			fmt.Printf("Error during OCF iteration for %s: %v\n", inputFile, err)
		}

		fmt.Printf("  Decoded %d messages from %s\n", messageCount, inputFile)
		totalMessageCount += messageCount
	}

	fmt.Printf("Total: %d messages from %d files\n", totalMessageCount, len(inputFiles))

	// Write output based on format
	if outputExt == ".json" {
		writeJSON(*outputFile, allMessages, *prettyPrint)
	} else {
		writeCSV(*outputFile, allMessages)
	}

	fmt.Printf("Output written to: %s\n", *outputFile)
}

func writeJSON(outputFile string, allMessages []json.RawMessage, prettyPrint bool) {
	var outputData []byte
	var err error
	if prettyPrint {
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
}

func writeCSV(outputFile string, allMessages []json.RawMessage) {
	file, err := os.Create(outputFile)
	if err != nil {
		fmt.Printf("Error creating CSV file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	// Write header - flattened to events level, payload stays as JSON
	header := []string{
		"playerID",
		"gameID",
		"country",
		"batchID",
		"sdkVersion",
		"player_id",
		"session_id",
		"device_id",
		"device_os",
		"device_model",
		"app_version",
		"event_id",
		"event_name",
		"timestamp",
		"scene_name",
		"payload",
	}
	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing CSV header: %v\n", err)
		os.Exit(1)
	}

	eventCount := 0
	for _, msg := range allMessages {
		var eventMsg EventMessage
		if err := json.Unmarshal(msg, &eventMsg); err != nil {
			fmt.Printf("Warning: Could not parse message as EventMessage: %v\n", err)
			continue
		}

		// Iterate through eventGroups and events
		for _, group := range eventMsg.EventGroups {
			for _, event := range group.Events {
				// Convert payload to JSON string
				payloadStr := string(event.Payload)
				if payloadStr == "" || payloadStr == "null" {
					payloadStr = "{}"
				}

				row := []string{
					eventMsg.PlayerID,
					eventMsg.GameID,
					eventMsg.Country,
					eventMsg.BatchID,
					eventMsg.SDKVersion,
					group.PlayerID,
					group.SessionID,
					group.DeviceID,
					group.DeviceOS,
					group.DeviceModel,
					group.AppVersion,
					event.ID,
					event.EventName,
					formatValue(event.Timestamp),
					event.SceneName,
					payloadStr,
				}

				if err := writer.Write(row); err != nil {
					fmt.Printf("Error writing CSV row: %v\n", err)
					continue
				}
				eventCount++
			}
		}
	}

	fmt.Printf("Wrote %d events to CSV\n", eventCount)
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%v", val)
	case int, int64:
		return fmt.Sprintf("%d", val)
	default:
		return fmt.Sprintf("%v", val)
	}
}

func scanInputDirectory(dir string) ([]string, error) {
	var files []string

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		// Skip hidden files and common non-data files
		name := entry.Name()
		if strings.HasPrefix(name, ".") {
			continue
		}

		// Include all files (Avro files typically have no extension or .avro)
		filePath := filepath.Join(dir, name)
		files = append(files, filePath)
	}

	return files, nil
}
