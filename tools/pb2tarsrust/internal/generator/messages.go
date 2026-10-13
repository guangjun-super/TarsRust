package generator

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func doc(out *strings.Builder, d protoreflect.Descriptor) {
	comment := d.ParentFile().SourceLocations().ByDescriptor(d).LeadingComments
	for _, line := range strings.Split(strings.TrimSpace(comment), "\n") {
		if line != "" {
			fmt.Fprintf(out, "#[doc = %s]\n", rustQuote(strings.TrimSpace(line)))
		}
	}
}

func (g *generator) emitTypes(out *strings.Builder, messages protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors) error {
	for i := 0; i < enums.Len(); i++ {
		if err := g.emitEnum(out, enums.Get(i)); err != nil {
			return err
		}
	}
	for i := 0; i < messages.Len(); i++ {
		m := messages.Get(i)
		if m.IsMapEntry() {
			continue
		}
		if m.Extensions().Len() > 0 || m.ExtensionRanges().Len() > 0 {
			return fmt.Errorf("%s: protobuf extensions are unsupported", m.FullName())
		}
		doc(out, m)
		fmt.Fprintf(out, "#[derive(Clone, PartialEq, ::prost::Message)]\npub struct %s {\n", pascal(string(m.Name())))
		for j := 0; j < m.Fields().Len(); j++ {
			f := m.Fields().Get(j)
			if o := f.ContainingOneof(); o != nil && !o.IsSynthetic() {
				continue
			}
			name := fieldIdent(f)
			if err := g.reserve(string(m.FullName())+" fields", name, string(f.FullName())); err != nil {
				return err
			}
			if err := g.reserveFieldMethods(f, name); err != nil {
				return err
			}
			attr, typ, err := g.field(f, false)
			if err != nil {
				return err
			}
			doc(out, f)
			fmt.Fprintf(out, "#[prost(%s)]\npub %s: %s,\n", attr, name, typ)
		}
		for j := 0; j < m.Oneofs().Len(); j++ {
			o := m.Oneofs().Get(j)
			if o.IsSynthetic() {
				continue
			}
			name := ident(snake(string(o.Name())))
			if err := g.reserve(string(m.FullName())+" fields", name, string(o.FullName())); err != nil {
				return err
			}
			var tags []string
			for k := 0; k < o.Fields().Len(); k++ {
				tags = append(tags, strconv.Itoa(int(o.Fields().Get(k).Number())))
			}
			fmt.Fprintf(out, "#[prost(oneof=%s, tags=%s)]\npub %s: ::core::option::Option<%s>,\n", rustQuote(g.types[o.FullName()]), rustQuote(strings.Join(tags, ", ")), name, g.types[o.FullName()])
		}
		out.WriteString("}\n\n")
		if m.Messages().Len() > 0 || m.Enums().Len() > 0 || realOneofs(m) > 0 {
			fmt.Fprintf(out, "pub mod %s {\n", ident(snake(string(m.Name()))))
			if err := g.emitTypes(out, m.Messages(), m.Enums()); err != nil {
				return err
			}
			for j := 0; j < m.Oneofs().Len(); j++ {
				o := m.Oneofs().Get(j)
				if o.IsSynthetic() {
					continue
				}
				fmt.Fprintf(out, "#[derive(Clone, PartialEq, ::prost::Oneof)]\npub enum %s {\n", pascal(string(o.Name())))
				for k := 0; k < o.Fields().Len(); k++ {
					f := o.Fields().Get(k)
					name := pascal(string(f.Name()))
					if err := g.reserve(string(o.FullName())+" variants", name, string(f.FullName())); err != nil {
						return err
					}
					attr, typ, err := g.field(f, true)
					if err != nil {
						return err
					}
					doc(out, f)
					fmt.Fprintf(out, "#[prost(%s)]\n%s(%s),\n", attr, name, typ)
				}
				out.WriteString("}\n\n")
			}
			out.WriteString("}\n\n")
		}
	}
	return nil
}

func fieldIdent(f protoreflect.FieldDescriptor) string {
	name := ident(snake(string(f.Name())))
	// prost 0.14's enum-map helpers concatenate the raw identifier into a
	// method name without removing r#, unlike its scalar helpers.
	if f.IsMap() && f.MapValue().Kind() == protoreflect.EnumKind && strings.HasPrefix(name, "r#") {
		return strings.TrimPrefix(name, "r#") + "_"
	}
	return name
}

func (g *generator) reserveFieldMethods(f protoreflect.FieldDescriptor, name string) error {
	var methods []string
	if f.IsMap() {
		if f.MapValue().Kind() == protoreflect.EnumKind {
			methods = []string{"get_" + name, "insert_" + name}
		}
	} else if f.Kind() == protoreflect.EnumKind {
		prefix := "set_"
		if f.IsList() {
			prefix = "push_"
		}
		methods = []string{name, prefix + strings.TrimPrefix(name, "r#")}
	} else if f.Kind() != protoreflect.MessageKind && !f.IsList() && f.HasPresence() && f.Cardinality() != protoreflect.Required {
		methods = []string{name}
	}
	for _, method := range methods {
		if err := g.reserve(string(f.ContainingMessage().FullName())+" methods", method, string(f.FullName())); err != nil {
			return err
		}
	}
	return nil
}

func enumVariant(e protoreflect.EnumDescriptor, v protoreflect.EnumValueDescriptor) string {
	name := string(v.Name())
	prefix := strings.ToUpper(snake(string(e.Name()))) + "_"
	if trimmed := strings.TrimPrefix(name, prefix); trimmed != "" {
		name = trimmed
	}
	return pascal(name)
}

func (g *generator) emitEnum(out *strings.Builder, e protoreflect.EnumDescriptor) error {
	doc(out, e)
	fmt.Fprintf(out, "#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]\n#[repr(i32)]\npub enum %s {\n", pascal(string(e.Name())))
	seen := map[protoreflect.EnumNumber]bool{}
	for i := 0; i < e.Values().Len(); i++ {
		v := e.Values().Get(i)
		if seen[v.Number()] {
			continue
		} // Rust enums cannot contain duplicate discriminants.
		seen[v.Number()] = true
		name := enumVariant(e, v)
		if err := g.reserve(string(e.FullName())+" variants", name, string(v.FullName())); err != nil {
			return err
		}
		doc(out, v)
		fmt.Fprintf(out, "%s = %d,\n", name, v.Number())
	}
	out.WriteString("}\n\n")
	return nil
}

func (g *generator) base(f protoreflect.FieldDescriptor) (string, string, error) {
	switch f.Kind() {
	case protoreflect.DoubleKind:
		return "double", "f64", nil
	case protoreflect.FloatKind:
		return "float", "f32", nil
	case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind:
		return f.Kind().String(), "i32", nil
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return f.Kind().String(), "i64", nil
	case protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		return f.Kind().String(), "u32", nil
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return f.Kind().String(), "u64", nil
	case protoreflect.BoolKind:
		return "bool", "bool", nil
	case protoreflect.StringKind:
		return "string", "::prost::alloc::string::String", nil
	case protoreflect.BytesKind:
		return "bytes=\"vec\"", "::prost::alloc::vec::Vec<u8>", nil
	case protoreflect.EnumKind:
		return "enumeration=" + rustQuote(g.types[f.Enum().FullName()]), "i32", nil
	case protoreflect.MessageKind:
		return "message", g.types[f.Message().FullName()], nil
	default:
		return "", "", fmt.Errorf("%s: unsupported protobuf %s field", f.FullName(), f.Kind())
	}
}

func (g *generator) field(f protoreflect.FieldDescriptor, oneof bool) (string, string, error) {
	attr, typ, err := g.base(f)
	if err != nil {
		return "", "", err
	}
	if f.IsMap() {
		keyAttr, keyType, err := g.base(f.MapKey())
		if err != nil {
			return "", "", err
		}
		valAttr, valType, err := g.base(f.MapValue())
		if err != nil {
			return "", "", err
		}
		// prost's map grammar uses bare bytes and enumeration(type) rather than field attributes.
		if f.MapValue().Kind() == protoreflect.BytesKind {
			valAttr = "bytes"
		}
		if f.MapValue().Kind() == protoreflect.EnumKind {
			valAttr = "enumeration(" + g.types[f.MapValue().Enum().FullName()] + ")"
		}
		attr = "map=" + rustQuote(keyAttr+", "+valAttr)
		typ = "::std::collections::HashMap<" + keyType + ", " + valType + ">"
	} else {
		if f.Kind() == protoreflect.MessageKind && !f.IsList() && recursive(f.ContainingMessage(), f.Message(), map[protoreflect.FullName]bool{}) {
			attr += ", boxed"
			typ = "::prost::alloc::boxed::Box<" + typ + ">"
		}
		if !oneof {
			switch {
			case f.IsList():
				attr += ", repeated"
				typ = "::prost::alloc::vec::Vec<" + typ + ">"
				if f.Kind() != protoreflect.MessageKind && f.Kind() != protoreflect.StringKind && f.Kind() != protoreflect.BytesKind {
					attr += ", packed=" + rustQuote(strconv.FormatBool(f.IsPacked()))
				}
			case f.Cardinality() == protoreflect.Required:
				attr += ", required"
			case f.HasPresence():
				attr += ", optional"
				typ = "::core::option::Option<" + typ + ">"
			}
			if f.HasDefault() {
				attr += ", default=" + rustQuote(defaultValue(f))
			}
		}
	}
	attr += ", tag=" + rustQuote(strconv.Itoa(int(f.Number())))
	return attr, typ, nil
}

// Box only singular message edges that can lead back to the owning message.
// Repeated/map edges already provide indirection and break Rust size cycles.
func recursive(owner, target protoreflect.MessageDescriptor, seen map[protoreflect.FullName]bool) bool {
	if owner.FullName() == target.FullName() {
		return true
	}
	if seen[target.FullName()] {
		return false
	}
	seen[target.FullName()] = true
	for i := 0; i < target.Fields().Len(); i++ {
		f := target.Fields().Get(i)
		if f.Kind() == protoreflect.MessageKind && !f.IsList() && !f.IsMap() && recursive(owner, f.Message(), seen) {
			return true
		}
	}
	return false
}

func defaultValue(f protoreflect.FieldDescriptor) string {
	v := f.Default()
	switch f.Kind() {
	case protoreflect.EnumKind:
		// Resolve an alias to the first variant retained in the Rust enum.
		return enumVariant(f.Enum(), f.Enum().Values().ByNumber(v.Enum()))
	case protoreflect.BytesKind:
		var s strings.Builder
		s.WriteString("b\"")
		for _, b := range v.Bytes() {
			fmt.Fprintf(&s, "\\x%02x", b)
		}
		s.WriteByte('"')
		return s.String()
	case protoreflect.DoubleKind, protoreflect.FloatKind:
		if math.IsInf(v.Float(), 1) {
			return "inf"
		}
		if math.IsInf(v.Float(), -1) {
			return "-inf"
		}
		if math.IsNaN(v.Float()) {
			return "nan"
		}
	}
	return fmt.Sprint(v.Interface())
}
