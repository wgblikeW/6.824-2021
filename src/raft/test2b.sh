#!/bin/bash

echo "TestBasicAgree2B"
go test -run ^TestBasicAgree2B$ | grep ok
echo "TestRPCBytes2B"
go test -run ^TestRPCBytes2B$ | grep ok
echo "TestFailAgree2B"
go test -run ^TestFailAgree2B$ | grep ok
echo "TestFailNoAgree2B"
go test -run ^TestFailNoAgree2B$ | grep ok
echo "TestConcurrentStarts2B"
go test -run ^TestConcurrentStarts2B$ | grep ok
echo "TestRejoin2B"
go test -run ^TestRejoin2B$ | grep ok
echo "TestBackup2B"
go test -run ^TestBackup2B$ | grep ok
echo "TestCount2B"
go test -run ^TestCount2B$ | grep ok