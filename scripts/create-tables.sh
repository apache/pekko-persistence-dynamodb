#!/bin/sh
set -o errexit -o nounset

DIR=$(dirname "$0")

JOURNAL_JSON=$(sed -e "s/\${TABLE_NAME}/$DDB_TABLE_JOURNAL/" "${DIR}/pekko-journal-table.json")
SNAPSHOT_JSON=$(sed -e "s/\${TABLE_NAME}/$DDB_TABLE_SNAPSHOT/" "${DIR}/pekko-snapshot-table.json")

echo "Creating journal table"
aws dynamodb create-table \
   --cli-input-json "$JOURNAL_JSON" \
   --endpoint-url "$DYNAMODB_ENDPOINT" \
   --region "$AWS_REGION"

echo "Creating snapshot table"
aws dynamodb create-table \
   --cli-input-json "$SNAPSHOT_JSON" \
   --endpoint-url "$DYNAMODB_ENDPOINT" \
   --region "$AWS_REGION"
