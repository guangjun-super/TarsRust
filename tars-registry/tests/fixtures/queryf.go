// Run from a temporary Go module replacing github.com/TarsCloud/TarsGo with
// a local TarsGo checkout. Writes unframed successful QueryF response bodies.
package main

import (
    "os"
    "github.com/TarsCloud/TarsGo/tars/protocol/codec"
    "github.com/TarsCloud/TarsGo/tars/protocol/res/endpointf"
)
func must(err error) { if err != nil { panic(err) } }
func main() {
    for i, name := range []string{"same_group.bin", "same_set.bin"} {
        b := codec.NewBuffer()
        must(b.WriteInt32(0, 0))
        tag := byte(2+i)
        must(b.WriteHead(codec.LIST, tag)); must(b.WriteInt32(1, 0))
        ep := endpointf.EndpointF{Host:"127.0.0.1", Port:12345, Timeout:3000, Istcp:1, Grid:7, Qos:9, Weight:42, WeightType:1, AuthType:2, SetId:"sz.area.1"}
        must(ep.WriteBlock(b, 0))
        must(b.WriteHead(codec.LIST, tag+1)); must(b.WriteInt32(0, 0))
        must(os.WriteFile(name, b.ToBytes(), 0644))
    }
}
