#!/bin/bash

cd $(dirname $0)/..

PID_FILE="target/dynamo.pid"

if [ ! -f dynamodb-local/DynamoDBLocal.jar ]; then
    scripts/get-dynamodb-local
fi

echo "Starting up dynamodb"
rm -f $PID_FILE

nohup java -Djava.library.path=./dynamodb-local -jar ./dynamodb-local/DynamoDBLocal.jar -inMemory -sharedDb > target/dynamodb-output.log 2>&1 &
echo $! > $PID_FILE
echo -n "Started up dynamodb, pid is "
cat $PID_FILE

echo "now start sbt and run 'test:run' for a load test or 'test' for the test suite"
