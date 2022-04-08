#!/bin/bash

echo "TestSnapshotRPC3B"
go test -run ^TestSnapshotRPC3B$ | grep ok
echo "TestSnapshotSize3B"
go test -run ^TestSnapshotSize3B$ | grep ok
echo "TestSpeed3B"
go test -run ^TestSpeed3B$ | grep ok
echo "TestSnapshotRecover3B"
go test -run ^TestSnapshotRecover3B$ | grep ok
echo "TestSnapshotRecoverManyClients3B"
go test -run ^TestSnapshotRecoverManyClients3B$ | grep ok
echo "TestSnapshotUnreliable3B"
go test -run ^TestSnapshotUnreliable3B$ | grep ok
echo "TestSnapshotUnreliableRecover3B"
go test -run ^TestSnapshotUnreliableRecover3B$ | grep ok
echo "TestSnapshotUnreliableRecoverConcurrentPartition3B"
go test -run ^TestSnapshotUnreliableRecoverConcurrentPartition3B$ | grep ok
echo "TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B"
go test -run ^TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B$ | grep ok