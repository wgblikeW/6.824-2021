echo "TestPersist22C"
loop=10
for i in $(seq 1 1 $loop)
do
    echo $i
    output=$(go test -run ^TestPersist22C$)
    count=1
    if echo "$output" | grep ok; then
        count=count+1
    else 
        echo $output > output_$i.txt
    fi
done

echo "scale=2; $count/$loop*100" | bc