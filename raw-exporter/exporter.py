#!/usr/bin/env python3
"""
Avro to CSV Exporter for Raw Events

Downloads Avro files from S3, parses nested JSON event data,
and exports to a unified CSV file.
"""

import argparse
import base64
import csv
import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import boto3
import fastavro
import msgpack


def parse_args():
    parser = argparse.ArgumentParser(
        description="Export Avro raw events from S3 to CSV"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name",
    )
    parser.add_argument(
        "--prefix",
        required=True,
        help="S3 key prefix (path to Avro files)",
    )
    parser.add_argument(
        "--profile",
        required=True,
        help="AWS SSO profile name",
    )
    parser.add_argument(
        "--output",
        default="output.csv",
        help="Output CSV file path (default: output.csv)",
    )
    return parser.parse_args()


def list_avro_files(s3_client, bucket: str, prefix: str) -> list[str]:
    """List all .avro files in the given S3 bucket/prefix."""
    avro_keys = []
    paginator = s3_client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".avro"):
                avro_keys.append(key)

    return avro_keys


def download_file(s3_client, bucket: str, key: str, local_path: Path) -> None:
    """Download a file from S3 to local path."""
    s3_client.download_file(bucket, key, str(local_path))


def ms_to_utc_datetime(timestamp_ms) -> str:
    """Convert milliseconds timestamp to UTC datetime string."""
    if timestamp_ms is None:
        return ""

    # Normalize input types (string, int, float)
    if isinstance(timestamp_ms, str):
        ts = timestamp_ms.strip()
        if not ts:
            return ""
        try:
            timestamp_ms = float(ts)
        except ValueError:
            return ""

    try:
        timestamp_ms = float(timestamp_ms)
    except (TypeError, ValueError):
        return ""

    dt = datetime.fromtimestamp(timestamp_ms / 1000, tz=timezone.utc)
    return dt.isoformat()


def decode_message(message_field) -> dict | None:
    """
    Decode message field trying multiple formats:
    1. MessagePack (binary)
    2. Raw JSON (UTF-8 string)
    3. Base64-encoded JSON
    """
    if not message_field:
        return None

    # Handle bytes (could be msgpack or raw data)
    if isinstance(message_field, bytes):
        # Try MessagePack first (0xa0-0xbf = fixstr, 0x80-0x8f = fixmap)
        try:
            data = msgpack.unpackb(message_field, raw=False)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

        # Try as raw JSON bytes
        try:
            data = json.loads(message_field.decode("utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    # Handle string
    if isinstance(message_field, str):
        # Try as raw JSON string
        try:
            data = json.loads(message_field)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

        # Try base64-encoded JSON
        try:
            decoded = base64.b64decode(message_field)
            data = json.loads(decoded.decode("utf-8"))
            if isinstance(data, dict):
                return data
        except Exception:
            pass

        # Try base64-encoded MessagePack
        try:
            decoded = base64.b64decode(message_field)
            data = msgpack.unpackb(decoded, raw=False)
            if isinstance(data, dict):
                return data
        except Exception:
            pass

    return None


def process_avro_file(avro_path: Path) -> list[dict]:
    """Parse an Avro file and flatten events into rows."""
    rows = []
    decode_failures = 0

    with open(avro_path, "rb") as f:
        reader = fastavro.reader(f)

        for record in reader:
            message_field = record.get("message", "")
            data = decode_message(message_field)

            if data is None:
                decode_failures += 1
                continue

            # Extract top-level fields
            player_id = data.get("playerID", "")
            game_id = data.get("gameID", "")
            country = data.get("country", "")
            batch_id = data.get("batchID", "")
            sdk_version = data.get("sdkVersion", "")

            # Process each event group
            for event_group in data.get("eventGroups", []):
                session_id = event_group.get("session_id", "")
                device_id = event_group.get("device_id", "")
                device_os = event_group.get("device_os", "")
                device_model = event_group.get("device_model", "")
                app_version = event_group.get("app_version", "")

                # Process each event within the group
                for event in event_group.get("events", []):
                    timestamp_ms = event.get("timestamp", 0)
                    event_timestamp = ms_to_utc_datetime(timestamp_ms) if timestamp_ms else ""

                    payload = event.get("payload", {})
                    payload_str = json.dumps(payload) if payload else ""

                    row = {
                        "playerID": player_id,
                        "gameID": game_id,
                        "country": country,
                        "session_id": session_id,
                        "device_id": device_id,
                        "device_os": device_os,
                        "device_model": device_model,
                        "app_version": app_version,
                        "event_id": event.get("id", ""),
                        "event_name": event.get("event_name", ""),
                        "event_timestamp": event_timestamp,
                        "event_timestamp_ref_utc": event.get("timestamp_ref_utc", ""),
                        "scene_name": event.get("scene_name", ""),
                        "payload": payload_str,
                        "batchID": batch_id,
                        "sdkVersion": sdk_version,
                    }
                    rows.append(row)

    if decode_failures > 0:
        print(f"  (skipped {decode_failures} records with decode failures)", file=sys.stderr)

    return rows


def write_csv(rows: list[dict], output_path: str) -> None:
    """Write rows to CSV file."""
    if not rows:
        print("Warning: No rows to write", file=sys.stderr)
        return

    fieldnames = [
        "playerID",
        "gameID",
        "country",
        "session_id",
        "device_id",
        "device_os",
        "device_model",
        "app_version",
        "event_id",
        "event_name",
        "event_timestamp",
        "event_timestamp_ref_utc",
        "scene_name",
        "payload",
        "batchID",
        "sdkVersion",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, quoting=csv.QUOTE_MINIMAL)
        writer.writeheader()
        writer.writerows(rows)


def main():
    args = parse_args()

    # Create boto3 session with SSO profile
    print(f"Using AWS profile: {args.profile}")
    session = boto3.Session(profile_name=args.profile)
    s3_client = session.client("s3")

    # List Avro files
    print(f"Listing Avro files in s3://{args.bucket}/{args.prefix}")
    avro_keys = list_avro_files(s3_client, args.bucket, args.prefix)
    print(f"Found {len(avro_keys)} Avro file(s)")

    if not avro_keys:
        print("No Avro files found. Exiting.")
        sys.exit(1)

    # Process files
    all_rows = []

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)

        for i, key in enumerate(avro_keys, 1):
            filename = Path(key).name
            local_file = temp_path / filename

            print(f"[{i}/{len(avro_keys)}] Downloading: {filename}")
            download_file(s3_client, args.bucket, key, local_file)

            print(f"[{i}/{len(avro_keys)}] Processing: {filename}")
            rows = process_avro_file(local_file)
            all_rows.extend(rows)
            print(f"  -> Extracted {len(rows)} events")

    # Write CSV
    print(f"\nWriting {len(all_rows)} total events to {args.output}")
    write_csv(all_rows, args.output)
    print("Done!")


if __name__ == "__main__":
    main()
