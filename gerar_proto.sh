#!/usr/bin/env bash
cd "$(dirname "$0")"
python3 -m grpc_tools.protoc -Iproto --python_out=. --grpc_python_out=. proto/raft.proto
