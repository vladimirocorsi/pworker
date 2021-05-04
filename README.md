# pWorker
pWorker is a worker pool with fixed buffer size where submitted work items are assigned a partition.
Work items of the same partition are processed serially in order of arrival.

## Example
```go
numPartitions := 10 //try replace this
in, out := pworker.MakePWorker(numPartitions, numPartitions*2,
    func(arg interface{}) (interface{}, error) {
        i := arg.(int)
        time.Sleep(100 * time.Millisecond)
        return i * i, nil
    })
var wg sync.WaitGroup
wg.Add(1)
go func() {
    for result := range out {
        fmt.Printf("Received %d \n", result.Output)
    }
    wg.Done()
    fmt.Println("finished reading")
}()
for i := 0; i < 100; i++ {
    pId := i % numPartitions
    in <- pworker.Work{PartitionId: pId, Input: i}
}
close(in)
fmt.Println("finished writing")
wg.Wait()
```
  
