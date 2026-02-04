//go:build metrics2csv

package main

import (
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// MetricsBatch represents the top-level object in the JSON array
type MetricsBatch struct {
	ID            map[string]string `json:"_id"`
	PlayerID      string            `json:"playerID"`
	GameID        string            `json:"gameID"`
	Country       string            `json:"country"`
	MetricMessage []MetricMessage   `json:"metricMessage"`
	BatchID       string            `json:"batchID"`
	SDKVersion    string            `json:"sdkVersion"`
}

// MetricMessage represents each metric event
type MetricMessage struct {
	ID         string                 `json:"id"`
	MetricName string                 `json:"metric_name"`
	Timestamp  int64                  `json:"timestamp"`
	Payload    map[string]interface{} `json:"payload"`
}

func main() {
	inputFile := flag.String("input", "", "Input JSON file path (JSON array format)")
	outputFile := flag.String("output", "", "Output CSV file path (defaults to input filename with .csv extension)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Usage: go run metrics2csv.go -input <json_file> [-output <csv_file>]")
		os.Exit(1)
	}

	// Read the entire JSON file
	data, err := os.ReadFile(*inputFile)
	if err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	// Parse JSON array
	var batches []MetricsBatch
	if err := json.Unmarshal(data, &batches); err != nil {
		fmt.Printf("Error parsing JSON: %v\n", err)
		os.Exit(1)
	}

	// First pass: collect all unique payload keys
	payloadKeys := make(map[string]bool)
	for _, batch := range batches {
		for _, msg := range batch.MetricMessage {
			for key := range msg.Payload {
				payloadKeys[key] = true
			}
		}
	}

	// Sort payload keys for consistent column order
	sortedPayloadKeys := sortMapKeys(payloadKeys)

	// Determine output file path
	outPath := *outputFile
	if outPath == "" {
		baseName := filepath.Base(*inputFile)
		baseName = strings.TrimSuffix(baseName, filepath.Ext(baseName))
		outPath = filepath.Join(filepath.Dir(*inputFile), baseName+".csv")
	}

	// Create CSV file
	csvFile, err := os.Create(outPath)
	if err != nil {
		fmt.Printf("Error creating CSV file: %v\n", err)
		os.Exit(1)
	}
	defer csvFile.Close()

	writer := csv.NewWriter(csvFile)
	defer writer.Flush()

	// Build header
	header := []string{
		// Parent-level fields
		"_id",
		"playerID",
		"gameID",
		"country",
		"batchID",
		"sdkVersion",
		// MetricMessage level fields
		"metric_id",
		"metric_name",
		"timestamp",
	}

	// Add payload columns
	for _, key := range sortedPayloadKeys {
		header = append(header, "payload_"+key)
	}

	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		os.Exit(1)
	}

	// Write data rows - one row per metricMessage
	rowCount := 0
	for _, batch := range batches {
		// Get _id value
		oid := ""
		if batch.ID != nil {
			if v, ok := batch.ID["$oid"]; ok {
				oid = v
			}
		}

		for _, msg := range batch.MetricMessage {
			row := []string{
				oid,
				batch.PlayerID,
				batch.GameID,
				batch.Country,
				batch.BatchID,
				batch.SDKVersion,
				msg.ID,
				msg.MetricName,
				fmt.Sprintf("%d", msg.Timestamp),
			}

			// Add payload values in order
			for _, key := range sortedPayloadKeys {
				val := ""
				if v, ok := msg.Payload[key]; ok {
					val = formatValue(v)
				}
				row = append(row, val)
			}

			if err := writer.Write(row); err != nil {
				fmt.Printf("Error writing row: %v\n", err)
				continue
			}
			rowCount++
		}
	}

	fmt.Printf("Converted %d metric messages from %d batches to CSV: %s\n", rowCount, len(batches), outPath)
}

func sortMapKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func formatValue(v interface{}) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case float64:
		if val == float64(int64(val)) {
			return fmt.Sprintf("%d", int64(val))
		}
		return fmt.Sprintf("%v", val)
	case int:
		return fmt.Sprintf("%d", val)
	case int64:
		return fmt.Sprintf("%d", val)
	case string:
		return val
	case bool:
		return fmt.Sprintf("%t", val)
	case map[string]interface{}:
		// For nested objects, serialize to JSON
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(jsonBytes)
	case []interface{}:
		// For arrays, serialize to JSON
		jsonBytes, err := json.Marshal(val)
		if err != nil {
			return fmt.Sprintf("%v", val)
		}
		return string(jsonBytes)
	default:
		return fmt.Sprintf("%v", v)
	}
}
