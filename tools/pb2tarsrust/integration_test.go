package pb2tarsrust_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// These tests catch broken plugin discovery/protocol, invalid generated Rust,
// incorrect protobuf wire encoding, and incorrect Tars dispatch or RPC names.
func TestProtocPlugin(t *testing.T) {
	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skip("protoc is required")
	}
	tmp := t.TempDir()
	plugin := filepath.Join(tmp, "protoc-gen-tarsrust")
	run(t, exec.Command("go", "build", "-o", plugin, "./protoc-gen-tarsrust"))
	for _, module := range []string{"crate::pb", "crate::wire", "crate"} {
		t.Run(module, func(t *testing.T) { testGeneratedRust(t, plugin, module) })
	}
}

func testGeneratedRust(t *testing.T, plugin, module string) {
	t.Helper()
	tmp := t.TempDir()
	out := filepath.Join(tmp, "src", "pb")
	if err := os.MkdirAll(out, 0755); err != nil {
		t.Fatal(err)
	}
	args := []string{"--proto_path=testdata", "--tarsrust_out=" + out, "hello.proto", "more.proto", "legacy.proto", "empty.proto", "shadowing.proto"}
	if module != "crate::pb" {
		args = append(args, "--tarsrust_opt=module="+module)
	}
	protoc := exec.Command("protoc", args...)
	protoc.Env = append(os.Environ(), "PATH="+filepath.Dir(plugin)+string(os.PathListSeparator)+os.Getenv("PATH"))
	run(t, protoc)
	for _, name := range []string{"mod.rs", "example.v1.rs", "common.rs", "legacy.rs", "_root.rs"} {
		if _, err := os.Stat(filepath.Join(out, name)); err != nil {
			t.Fatal(err)
		}
	}
	if os.Getenv("PB2TARSRUST_TEST_RUST") != "1" {
		return
	}
	repo, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := os.ReadFile("testdata/rust/Cargo.toml")
	if err != nil {
		t.Fatal(err)
	}
	manifest = []byte(strings.ReplaceAll(string(manifest), "@TARS_PATH@", filepath.Join(repo, "tars")))
	if err := os.WriteFile(filepath.Join(tmp, "Cargo.toml"), manifest, 0644); err != nil {
		t.Fatal(err)
	}
	lib, err := os.ReadFile("testdata/rust/lib.rs")
	if err != nil {
		t.Fatal(err)
	}
	switch module {
	case "crate::wire":
		lib = []byte(strings.ReplaceAll(strings.ReplaceAll(string(lib), "pub mod pb;", "#[path = \"pb/mod.rs\"] pub mod wire;"), "super::pb", "super::wire"))
	case "crate":
		lib = []byte(strings.ReplaceAll(strings.ReplaceAll(string(lib), "pub mod pb;", "include!(\"pb/mod.rs\");"), "super::pb::", "super::"))
	}
	if err := os.WriteFile(filepath.Join(tmp, "src", "lib.rs"), lib, 0644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("cargo", "test", "--manifest-path", filepath.Join(tmp, "Cargo.toml"))
	cmd.Env = append(os.Environ(), "CARGO_TARGET_DIR="+filepath.Join(repo, "target", "pb2tarsrust"))
	run(t, cmd)
}

func TestUnsupportedInputs(t *testing.T) {
	if _, err := exec.LookPath("protoc"); err != nil {
		t.Skip("protoc is required")
	}
	tmp := t.TempDir()
	plugin := filepath.Join(tmp, "protoc-gen-tarsrust")
	run(t, exec.Command("go", "build", "-o", plugin, "./protoc-gen-tarsrust"))
	for _, tc := range []struct{ name, source, parameter, want string }{
		{"stream", `syntax="proto3"; message M {} service S { rpc Call(stream M) returns(M); }`, "", "streaming"},
		{"option", `syntax="proto3"; message M {}`, "unknown=true", "unknown option"},
		{"group", `syntax="proto2"; message M { optional group G = 1 { optional int32 n = 2; } }`, "", "group"},
		{"collision", `syntax="proto3"; message foo_bar {} message FooBar {}`, "", "collision"},
		{"output_collision", `syntax="proto3"; package mod; message M {}`, "", "collision"},
		{"invalid_module", `syntax="proto3"; message M {}`, "module=not a Rust path", "invalid Rust module"},
		{"enum_setter_collision", `syntax="proto3"; enum Color { ZERO=0; RED=1; } message M { Color color=1; optional int32 set_color=2; }`, "", "collision"},
		{"enum_push_collision", `syntax="proto3"; enum Color { ZERO=0; RED=1; } message M { repeated Color colors=1; optional int32 push_colors=2; }`, "", "collision"},
		{"enum_map_collision", `syntax="proto3"; enum Color { ZERO=0; RED=1; } message M { map<string, Color> colors=1; optional int32 get_colors=2; }`, "", "collision"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			name := filepath.Join(tmp, tc.name+".proto")
			if err := os.WriteFile(name, []byte(tc.source), 0644); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command("protoc", "--plugin=protoc-gen-tarsrust="+plugin, "--proto_path="+tmp, "--tarsrust_out="+tmp, "--tarsrust_opt="+tc.parameter, name)
			output, err := cmd.CombinedOutput()
			if err == nil || !strings.Contains(string(output), tc.want) {
				t.Fatalf("expected %q error, got %v: %s", tc.want, err, output)
			}
		})
	}
}

func run(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", cmd, err, output)
	}
	t.Logf("%s\n%s", cmd, output)
}
