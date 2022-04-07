#!/bin/bash

echo "TestBasic3A"
go test -run ^TestBasic3A$ | grep ok
echo "TestSpeed3A"
go test -run ^TestSpeed3A$ | grep ok
echo "TestConcurrent3A"
go test -run ^TestConcurrent3A$ | grep ok
echo "TestUnreliable3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestUnreliableOneKey3A"
go test -run ^TestSpeed3A$ | grep ok
echo "TestOnePartition3A"
go test -run ^TestConcurrent3A$ | grep ok
echo "TestManyPartitionsOneClient3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestManyPartitionsManyClients3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistOneClient3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistConcurrent3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistConcurrentUnreliable3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistPartition3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistPartitionUnreliable3A"
go test -run ^TestUnreliable3A$ | grep ok
echo "TestPersistPartitionUnreliableLinearizable3A"
go test -run ^TestUnreliable3A$ | grep ok