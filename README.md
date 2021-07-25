# goks

```
func main() {
    sb := goks.NewStreamBuilder() // load config
    
    s := sb.Stream("input-topic")
        .Filter(func (kvc KeyValueHeaders<string, jsonstruct>) bool {
            var key string = kvc.Key
            var value jsonstruct = kvc.Value
        })
        .Map()  
        .Peek()
    t := sb.Table("normal-table")
        .Filter()
    
    topology := sb.Build()
    goks.Start(topology)
}
```