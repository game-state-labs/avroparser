# Raw Events Exporter and Analyzer

This script is used to export raw events from a s3 bucket and analyze them.

## Usage

```bash
uv run python exporter.py \
    --bucket dataplane-raw-events-production \ # the s3 bucket containing the raw events
    --prefix "data-plane/contextualizer/nukebox_ftc_64aae2-partition-0" \ # the prefix of the raw events
    --profile GSL-Prod \ # the aws profile to use
    --output out/output.csv # the output file
```