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
echo "Full Test"
go test -run 2C | grep ok



# echo "TestReliableChurn2C"
# loop=10
# for i in $(seq 1 1 $loop)
# do
#     echo $i
#     output=$(go test -run ^TestReliableChurn2C$)
#     count=1
#     if echo "$output" | grep ok; then
#         count=count+1
#     else 
#         echo $output > output_$i.txt
#     fi
# done

# echo "scale=2; $count/$loop*100" | bc

