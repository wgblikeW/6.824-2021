#!/bin/bash

echo "TestPersist12C"
go test -run ^TestPersist12C$ | grep ok
echo "TestPersist22C"
go test -run ^TestPersist22C$ | grep ok
echo "TestPersist32C"
go test -run ^TestPersist32C$ | grep ok
echo "TestFigure82C"
go test -run ^TestFigure82C$ | grep ok
echo "TestUnreliableAgree2C"
go test -run ^TestUnreliableAgree2C$ | grep ok
echo "TestFigure8Unreliable2C"
go test -run ^TestFigure8Unreliable2C$ | grep ok
echo "TestReliableChurn2C"
go test -run ^TestReliableChurn2C$ | grep ok
echo "TestUnreliableChurn2C"
go test -run ^TestUnreliableChurn2C$ | grep ok

