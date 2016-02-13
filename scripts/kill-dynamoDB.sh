#!/bin/bash

cd $(dirname $0)/..

PID_FILE="target/dynamo.pid"

if [ -s "$PID_FILE" ]; then
  PID="$(cat $PID_FILE)"
  echo "killing process $PID"
  kill "$PID"
else
  echo "no pid file present (at $PID_FILE)"
fi
