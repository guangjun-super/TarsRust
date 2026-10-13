package generator

import (
	"fmt"
	"sort"
	"strings"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/pluginpb"
)

type generator struct {
	root  string
	types map[protoreflect.FullName]string
	names map[string]string
}

// Generate consumes protoc descriptors; all failures use the plugin response's
// error field so protoc can report them without corrupting its binary protocol.
func Generate(req *pluginpb.CodeGeneratorRequest) *pluginpb.CodeGeneratorResponse {
	resp := &pluginpb.CodeGeneratorResponse{SupportedFeatures: proto.Uint64(uint64(pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL))}
	files, err := generate(req)
	if err != nil {
		resp.Error = proto.String(err.Error())
	} else {
		resp.File = files
	}
	return resp
}

func generate(req *pluginpb.CodeGeneratorRequest) ([]*pluginpb.CodeGeneratorResponse_File, error) {
	g := &generator{root: "crate::pb", types: map[protoreflect.FullName]string{}, names: map[string]string{}}
	for _, opt := range strings.Split(req.GetParameter(), ",") {
		if opt == "" {
			continue
		}
		key, value, ok := strings.Cut(opt, "=")
		if key != "module" || !ok {
			return nil, fmt.Errorf("unknown option %q (supported: module=crate::pb)", opt)
		}
		if !validModule(value) {
			return nil, fmt.Errorf("invalid Rust module path %q", value)
		}
		g.root = value
	}
	if len(req.FileToGenerate) == 0 {
		return nil, fmt.Errorf("no files to generate")
	}
	registry, err := protodesc.NewFiles(&descriptorpb.FileDescriptorSet{File: req.ProtoFile})
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	targets := map[string]bool{}
	var ordered []protoreflect.FileDescriptor
	var visit func(protoreflect.FileDescriptor) error
	visit = func(file protoreflect.FileDescriptor) error {
		if selected[file.Path()] {
			return nil
		}
		selected[file.Path()] = true
		if file.Syntax() != protoreflect.Proto2 && file.Syntax() != protoreflect.Proto3 {
			return fmt.Errorf("%s: only proto2 and proto3 are supported", file.Path())
		}
		if file.Extensions().Len() > 0 {
			return fmt.Errorf("%s: protobuf extensions are unsupported", file.Path())
		}
		for i := 0; i < file.Imports().Len(); i++ {
			if err := visit(file.Imports().Get(i).FileDescriptor); err != nil {
				return err
			}
		}
		ordered = append(ordered, file)
		return nil
	}
	for _, name := range req.FileToGenerate {
		file, err := registry.FindFileByPath(name)
		if err != nil {
			return nil, err
		}
		targets[name] = true
		if err := visit(file); err != nil {
			return nil, err
		}
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].Path() < ordered[j].Path() })
	packages := map[string]*strings.Builder{}
	for _, file := range ordered {
		pkg := string(file.Package())
		if packages[pkg] == nil {
			packages[pkg] = &strings.Builder{}
		}
		scope := g.root
		for _, part := range strings.Split(pkg, ".") {
			if part == "" {
				continue
			}
			name := ident(snake(part))
			if err := g.reserve(scope, name, "package "+scope+"."+part); err != nil {
				return nil, err
			}
			scope += "::" + name
		}
		if err := g.index(file.Messages(), file.Enums(), scope); err != nil {
			return nil, err
		}
		if targets[file.Path()] {
			for i := 0; i < file.Services().Len(); i++ {
				s := file.Services().Get(i)
				for _, name := range []string{pascal(string(s.Name())), pascal(string(s.Name())) + "Proxy", pascal(string(s.Name())) + "Servant"} {
					if err := g.reserve(scope, name, string(s.FullName())); err != nil {
						return nil, err
					}
				}
			}
		}
	}
	for _, file := range ordered {
		out := packages[string(file.Package())]
		fmt.Fprintf(out, "// Source: %s\n\n", file.Path())
		if err := g.emitTypes(out, file.Messages(), file.Enums()); err != nil {
			return nil, err
		}
		if targets[file.Path()] {
			for i := 0; i < file.Services().Len(); i++ {
				if err := g.emitService(out, file.Services().Get(i)); err != nil {
					return nil, err
				}
			}
		}
	}
	var keys []string
	for pkg := range packages {
		keys = append(keys, pkg)
	}
	sort.Strings(keys)
	var files []*pluginpb.CodeGeneratorResponse_File
	filenames := map[string]bool{"mod.rs": true}
	tree := &moduleTree{children: map[string]*moduleTree{}}
	for _, pkg := range keys {
		filename := pkg + ".rs"
		if pkg == "" {
			filename = "_root.rs"
		}
		if filenames[filename] {
			return nil, fmt.Errorf("output filename collision for package %q: %s", pkg, filename)
		}
		filenames[filename] = true
		files = append(files, outputFile(filename, packages[pkg].String()))
		node := tree
		if pkg != "" {
			for _, part := range strings.Split(pkg, ".") {
				name := ident(snake(part))
				if node.children[name] == nil {
					node.children[name] = &moduleTree{children: map[string]*moduleTree{}}
				}
				node = node.children[name]
			}
		}
		node.filename = filename
	}
	var entry strings.Builder
	tree.emit(&entry)
	files = append(files, outputFile("mod.rs", entry.String()))
	return files, nil
}

func validModule(path string) bool {
	parts := strings.Split(path, "::")
	if len(parts) == 0 || parts[0] != "crate" {
		return false
	}
	for _, part := range parts[1:] {
		if part == "" || ident(part) != part {
			return false
		}
		for i, r := range part {
			if !(r == '_' || r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || i > 0 && r >= '0' && r <= '9') {
				return false
			}
		}
	}
	return true
}

func (g *generator) reserve(scope, name, origin string) error {
	key := scope + "::" + name
	if prev, ok := g.names[key]; ok && prev != origin {
		return fmt.Errorf("Rust name collision at %s: %s and %s", key, prev, origin)
	}
	g.names[key] = origin
	return nil
}

func (g *generator) index(messages protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors, scope string) error {
	for i := 0; i < enums.Len(); i++ {
		e := enums.Get(i)
		name := pascal(string(e.Name()))
		if err := g.reserve(scope, name, string(e.FullName())); err != nil {
			return err
		}
		g.types[e.FullName()] = scope + "::" + name
	}
	for i := 0; i < messages.Len(); i++ {
		m := messages.Get(i)
		if m.IsMapEntry() {
			continue
		}
		name := pascal(string(m.Name()))
		if err := g.reserve(scope, name, string(m.FullName())); err != nil {
			return err
		}
		g.types[m.FullName()] = scope + "::" + name
		if m.Messages().Len() > 0 || m.Enums().Len() > 0 || realOneofs(m) > 0 {
			module := ident(snake(string(m.Name())))
			if err := g.reserve(scope, module, string(m.FullName())+" module"); err != nil {
				return err
			}
			child := scope + "::" + module
			if err := g.index(m.Messages(), m.Enums(), child); err != nil {
				return err
			}
			for j := 0; j < m.Oneofs().Len(); j++ {
				o := m.Oneofs().Get(j)
				if !o.IsSynthetic() {
					if err := g.reserve(child, pascal(string(o.Name())), string(o.FullName())); err != nil {
						return err
					}
					g.types[o.FullName()] = child + "::" + pascal(string(o.Name()))
				}
			}
		}
	}
	return nil
}

func realOneofs(m protoreflect.MessageDescriptor) int {
	n := 0
	for i := 0; i < m.Oneofs().Len(); i++ {
		if !m.Oneofs().Get(i).IsSynthetic() {
			n++
		}
	}
	return n
}

type moduleTree struct {
	filename string
	children map[string]*moduleTree
}

func (m *moduleTree) emit(out *strings.Builder) {
	if m.filename != "" {
		fmt.Fprintf(out, "include!(%s);\n", rustQuote(m.filename))
	}
	var keys []string
	for key := range m.children {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, name := range keys {
		fmt.Fprintf(out, "pub mod %s {\n", name)
		m.children[name].emit(out)
		out.WriteString("}\n")
	}
}

func outputFile(name, content string) *pluginpb.CodeGeneratorResponse_File {
	return &pluginpb.CodeGeneratorResponse_File{Name: proto.String(name), Content: proto.String("// Generated by protoc-gen-tarsrust (pb2tarsrust). DO NOT EDIT.\n\n" + content)}
}
