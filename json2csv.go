//go:build json2csv

package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Firebase Analytics export event structure
type FirebaseEvent struct {
	EventDate                     string                 `json:"event_date"`
	EventTimestamp                string                 `json:"event_timestamp"`
	EventName                     string                 `json:"event_name"`
	EventParams                   []KeyValue             `json:"event_params"`
	EventPreviousTimestamp        string                 `json:"event_previous_timestamp"`
	EventBundleSequenceID         string                 `json:"event_bundle_sequence_id"`
	EventServerTimestampOffset    string                 `json:"event_server_timestamp_offset"`
	UserPseudoID                  string                 `json:"user_pseudo_id"`
	PrivacyInfo                   map[string]string      `json:"privacy_info"`
	UserProperties                []UserProperty         `json:"user_properties"`
	UserFirstTouchTimestamp       string                 `json:"user_first_touch_timestamp"`
	Device                        Device                 `json:"device"`
	Geo                           Geo                    `json:"geo"`
	AppInfo                       AppInfo                `json:"app_info"`
	TrafficSource                 TrafficSource          `json:"traffic_source"`
	StreamID                      string                 `json:"stream_id"`
	Platform                      string                 `json:"platform"`
	CollectedTrafficSource        map[string]interface{} `json:"collected_traffic_source"`
	IsActiveUser                  interface{}            `json:"is_active_user"`
	BatchEventIndex               string                 `json:"batch_event_index"`
	SessionTrafficSourceLastClick map[string]interface{} `json:"session_traffic_source_last_click"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value Value  `json:"value"`
}

type Value struct {
	StringValue interface{} `json:"string_value"`
	IntValue    interface{} `json:"int_value"`
	FloatValue  interface{} `json:"float_value"`
	DoubleValue interface{} `json:"double_value"`
}

type UserProperty struct {
	Key   string        `json:"key"`
	Value UserPropValue `json:"value"`
}

type UserPropValue struct {
	StringValue        interface{} `json:"string_value"`
	IntValue           interface{} `json:"int_value"`
	FloatValue         interface{} `json:"float_value"`
	DoubleValue        interface{} `json:"double_value"`
	SetTimestampMicros interface{} `json:"set_timestamp_micros"`
}

type Device struct {
	Category               string      `json:"category"`
	MobileBrandName        string      `json:"mobile_brand_name"`
	MobileModelName        string      `json:"mobile_model_name"`
	MobileMarketingName    string      `json:"mobile_marketing_name"`
	MobileOSHardwareModel  string      `json:"mobile_os_hardware_model"`
	OperatingSystem        string      `json:"operating_system"`
	OperatingSystemVersion string      `json:"operating_system_version"`
	AdvertisingID          string      `json:"advertising_id"`
	Language               string      `json:"language"`
	IsLimitedAdTracking    string      `json:"is_limited_ad_tracking"`
	TimeZoneOffsetSeconds  interface{} `json:"time_zone_offset_seconds"`
}

type Geo struct {
	City         string `json:"city"`
	Country      string `json:"country"`
	Continent    string `json:"continent"`
	Region       string `json:"region"`
	SubContinent string `json:"sub_continent"`
	Metro        string `json:"metro"`
}

type AppInfo struct {
	ID            string `json:"id"`
	Version       string `json:"version"`
	FirebaseAppID string `json:"firebase_app_id"`
	InstallSource string `json:"install_source"`
}

type TrafficSource struct {
	Medium string `json:"medium"`
	Source string `json:"source"`
}

func main() {
	inputFile := flag.String("input", "", "Input JSON file path (NDJSON format)")
	outputFile := flag.String("output", "", "Output CSV file path (defaults to input filename with .csv extension)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Usage: go run json2csv.go -input <json_file> [-output <csv_file>]")
		os.Exit(1)
	}

	// Open input file
	file, err := os.Open(*inputFile)
	if err != nil {
		fmt.Printf("Error opening file: %v\n", err)
		os.Exit(1)
	}
	defer file.Close()

	// First pass: collect all unique event_params and user_properties keys
	eventParamKeys := make(map[string]bool)
	userPropKeys := make(map[string]bool)

	scanner := bufio.NewScanner(file)
	// Increase buffer size for long lines
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, 10*1024*1024)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var event FirebaseEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			continue
		}

		for _, param := range event.EventParams {
			eventParamKeys[param.Key] = true
		}
		for _, prop := range event.UserProperties {
			userPropKeys[prop.Key] = true
		}
	}

	// Sort keys for consistent column order
	sortedEventParamKeys := sortKeys(eventParamKeys)
	sortedUserPropKeys := sortKeys(userPropKeys)

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
		"event_date",
		"event_timestamp",
		"event_name",
		"event_previous_timestamp",
		"event_bundle_sequence_id",
		"event_server_timestamp_offset",
		"user_pseudo_id",
		"user_first_touch_timestamp",
		// Device fields
		"device_category",
		"device_mobile_brand_name",
		"device_mobile_model_name",
		"device_mobile_marketing_name",
		"device_mobile_os_hardware_model",
		"device_operating_system",
		"device_operating_system_version",
		"device_advertising_id",
		"device_language",
		"device_is_limited_ad_tracking",
		"device_time_zone_offset_seconds",
		// Geo fields
		"geo_city",
		"geo_country",
		"geo_continent",
		"geo_region",
		"geo_sub_continent",
		"geo_metro",
		// App info fields
		"app_info_id",
		"app_info_version",
		"app_info_firebase_app_id",
		"app_info_install_source",
		// Traffic source fields
		"traffic_source_medium",
		"traffic_source_source",
		// Other fields
		"stream_id",
		"platform",
		"is_active_user",
		"batch_event_index",
		// Privacy info
		"privacy_analytics_storage",
		"privacy_ads_storage",
		"privacy_uses_transient_token",
	}

	// Add event_params columns
	for _, key := range sortedEventParamKeys {
		header = append(header, "param_"+key)
	}

	// Add user_properties columns
	for _, key := range sortedUserPropKeys {
		header = append(header, "user_prop_"+key)
	}

	if err := writer.Write(header); err != nil {
		fmt.Printf("Error writing header: %v\n", err)
		os.Exit(1)
	}

	// Second pass: write data rows
	file.Seek(0, 0)
	scanner = bufio.NewScanner(file)
	scanner.Buffer(buf, 10*1024*1024)

	rowCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		if strings.TrimSpace(line) == "" {
			continue
		}

		var event FirebaseEvent
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			fmt.Printf("Warning: Error parsing line: %v\n", err)
			continue
		}

		// Build event params map
		eventParamsMap := make(map[string]string)
		for _, param := range event.EventParams {
			eventParamsMap[param.Key] = getValue(param.Value)
		}

		// Build user properties map
		userPropsMap := make(map[string]string)
		for _, prop := range event.UserProperties {
			userPropsMap[prop.Key] = getUserPropValue(prop.Value)
		}

		row := []string{
			event.EventDate,
			event.EventTimestamp,
			event.EventName,
			event.EventPreviousTimestamp,
			event.EventBundleSequenceID,
			event.EventServerTimestampOffset,
			event.UserPseudoID,
			event.UserFirstTouchTimestamp,
			// Device fields
			event.Device.Category,
			event.Device.MobileBrandName,
			event.Device.MobileModelName,
			event.Device.MobileMarketingName,
			event.Device.MobileOSHardwareModel,
			event.Device.OperatingSystem,
			event.Device.OperatingSystemVersion,
			event.Device.AdvertisingID,
			event.Device.Language,
			event.Device.IsLimitedAdTracking,
			formatInterface(event.Device.TimeZoneOffsetSeconds),
			// Geo fields
			event.Geo.City,
			event.Geo.Country,
			event.Geo.Continent,
			event.Geo.Region,
			event.Geo.SubContinent,
			event.Geo.Metro,
			// App info fields
			event.AppInfo.ID,
			event.AppInfo.Version,
			event.AppInfo.FirebaseAppID,
			event.AppInfo.InstallSource,
			// Traffic source fields
			event.TrafficSource.Medium,
			event.TrafficSource.Source,
			// Other fields
			event.StreamID,
			event.Platform,
			formatBool(event.IsActiveUser),
			event.BatchEventIndex,
			// Privacy info
			event.PrivacyInfo["analytics_storage"],
			event.PrivacyInfo["ads_storage"],
			event.PrivacyInfo["uses_transient_token"],
		}

		// Add event_params values
		for _, key := range sortedEventParamKeys {
			row = append(row, eventParamsMap[key])
		}

		// Add user_properties values
		for _, key := range sortedUserPropKeys {
			row = append(row, userPropsMap[key])
		}

		if err := writer.Write(row); err != nil {
			fmt.Printf("Error writing row: %v\n", err)
			continue
		}
		rowCount++
	}

	if err := scanner.Err(); err != nil {
		fmt.Printf("Error reading file: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Converted %d events to CSV: %s\n", rowCount, outPath)
}

func sortKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func getValue(v Value) string {
	if v.StringValue != nil {
		return formatInterface(v.StringValue)
	}
	if v.IntValue != nil {
		return formatInterface(v.IntValue)
	}
	if v.FloatValue != nil {
		return formatInterface(v.FloatValue)
	}
	if v.DoubleValue != nil {
		return formatInterface(v.DoubleValue)
	}
	return ""
}

func getUserPropValue(v UserPropValue) string {
	if v.StringValue != nil {
		return formatInterface(v.StringValue)
	}
	if v.IntValue != nil {
		return formatInterface(v.IntValue)
	}
	if v.FloatValue != nil {
		return formatInterface(v.FloatValue)
	}
	if v.DoubleValue != nil {
		return formatInterface(v.DoubleValue)
	}
	return ""
}

func formatBool(v interface{}) string {
	switch val := v.(type) {
	case bool:
		return fmt.Sprintf("%t", val)
	case string:
		return val
	default:
		return fmt.Sprintf("%v", v)
	}
}

func formatInterface(v interface{}) string {
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
	default:
		return fmt.Sprintf("%v", v)
	}
}
