// protoc-gen-tarsrust generates protobuf messages and Tars RPC bindings for Rust.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/TarsCloud/TarsRust/tools/pb2tarsrust/internal/generator"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "protoc-gen-tarsrust:", err)
		os.Exit(1)
	}
}

func run() error {
	data, err := io.ReadAll(os.Stdin)
	if err != nil {
		return err
	}
	req := new(pluginpb.CodeGeneratorRequest)
	if err := proto.Unmarshal(data, req); err != nil {
		return fmt.Errorf("decode request: %w", err)
	}
	resp := generator.Generate(req)
	data, err = proto.Marshal(resp)
	if err != nil {
		return err
	}
	_, err = os.Stdout.Write(data)
	return err
}
