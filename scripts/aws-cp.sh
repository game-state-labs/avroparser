START_DATE="2025-12-21T00:00:00Z"
END_DATE="2026-02-06T23:59:59Z"
GAME_ID="xquads_scarfall_98dd1b-partition-0"
BUCKET_NAME="dataplane-raw-events-production"

PREFIX="data-plane/contextualizer/$GAME_ID/"
DEST_DIR="$(pwd)/input"

mkdir -p "$DEST_DIR"

OBJECT_KEYS=$(aws s3api list-objects-v2 --bucket "$BUCKET_NAME" --prefix "$PREFIX" --query "Contents[?LastModified>=\`$START_DATE\` && LastModified<=\`$END_DATE\`].Key" --output text)

for KEY in $OBJECT_KEYS; do
  aws s3 cp "s3://$BUCKET_NAME/$KEY" "$DEST_DIR"
done