#!/bin/bash

echo "TestInitialElection2A"
go test -run ^TestInitialElection2A$ | grep ok
echo "TestReElection2A"
go test -run ^TestReElection2A$ | grep ok
echo "TestManyElections2A"
go test -run ^TestManyElections2A$ | grep ok