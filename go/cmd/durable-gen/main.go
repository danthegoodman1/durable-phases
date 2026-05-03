package main

import (
	"bytes"
	"flag"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
)

type workflowDef struct {
	TypeName   string
	Name       string
	Version    int
	Input      string
	Output     string
	Common     string
	Phases     []phaseDef
	Queries    []queryDef
	Migrations []migrationDef
	Signals    []signalDef
	Timers     []timerDef
	Children   []childDef
}

type phaseDef struct {
	Name      string
	StateType string
	Run       bool
}

type queryDef struct {
	Name   string
	Output string
}

type migrationDef struct {
	From int
}

type signalDef struct {
	Name    string
	Payload string
	Global  bool
}

type timerDef struct {
	Name string
	At   string
}

type childDef struct {
	Name   string
	Handle string
}

func main() {
	dir := flag.String("dir", ".", "package directory")
	out := flag.String("out", "durable_gen.go", "output file")
	flag.Parse()
	if err := run(*dir, *out); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(dir, out string) error {
	pkg, workflows, err := parsePackage(dir)
	if err != nil {
		return err
	}
	rendered, err := render(pkg, workflows)
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, out), rendered, 0o644)
}

func parsePackage(dir string) (pkgName string, workflows []workflowDef, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			if asErr, ok := recovered.(error); ok {
				err = asErr
			} else {
				err = fmt.Errorf("%v", recovered)
			}
		}
	}()
	fset := token.NewFileSet()
	pkgs, err := parser.ParseDir(fset, dir, func(info os.FileInfo) bool {
		name := info.Name()
		return strings.HasSuffix(name, ".go") && !strings.HasSuffix(name, "_gen.go") && !strings.HasSuffix(name, "_test.go")
	}, parser.ParseComments)
	if err != nil {
		return "", nil, err
	}
	if len(pkgs) == 0 {
		return "", nil, fmt.Errorf("no Go package found in %s", dir)
	}
	var pkg *ast.Package
	for _, candidate := range pkgs {
		pkg = candidate
		break
	}
	workflowsByType := map[string]*workflowDef{}
	phasesByReceiver := map[string][]phaseDef{}
	var orphanPhases []phaseDef
	var orphanSignals []signalDef
	var orphanTimers []timerDef
	var orphanChildren []childDef
	var orphanQueries []queryDef
	var orphanMigrations []migrationDef
	for _, file := range pkg.Files {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.GenDecl:
				if n.Doc == nil {
					return true
				}
				for _, spec := range n.Specs {
					typeSpec, ok := spec.(*ast.TypeSpec)
					if !ok {
						continue
					}
					for _, line := range durableLines(n.Doc) {
						kind, attrs := parseDirective(line)
						switch kind {
						case "workflow":
							version, _ := strconv.Atoi(attrs["version"])
							if attrs["name"] == "" || version == 0 || attrs["input"] == "" || attrs["output"] == "" {
								panicError(fmt.Errorf("workflow %s is missing name/version/input/output", typeSpec.Name.Name))
							}
							common := attrs["common"]
							if common == "" {
								common = "struct{}"
							}
							workflowsByType[typeSpec.Name.Name] = &workflowDef{
								TypeName: typeSpec.Name.Name,
								Name:     attrs["name"],
								Version:  version,
								Input:    attrs["input"],
								Output:   attrs["output"],
								Common:   common,
							}
						case "phase":
							phase := phaseDef{Name: attrs["name"], StateType: attrs["state"], Run: attrs["run"] == "true"}
							if phase.Name == "" {
								panicError(fmt.Errorf("phase on %s is missing name", typeSpec.Name.Name))
							}
							if phase.StateType == "" && !phase.Run {
								phase.StateType = typeSpec.Name.Name
							}
							orphanPhases = append(orphanPhases, phase)
						case "":
						default:
							panicError(fmt.Errorf("unknown durable directive %q on %s", kind, typeSpec.Name.Name))
						}
					}
				}
			case *ast.FuncDecl:
				if n.Doc == nil {
					return true
				}
				recv := receiverName(n)
				for _, line := range durableLines(n.Doc) {
					kind, attrs := parseDirective(line)
					switch kind {
					case "phase":
						phase := phaseDef{Name: attrs["name"], StateType: attrs["state"], Run: attrs["run"] == "true"}
						if phase.Name == "" {
							panicError(fmt.Errorf("phase on %s is missing name", n.Name.Name))
						}
						if phase.StateType == "" && n.Type.Params != nil && n.Type.Params.NumFields() > 0 {
							phase.StateType = exprString(n.Type.Params.List[n.Type.Params.NumFields()-1].Type)
						}
						if recv != "" {
							phasesByReceiver[recv] = append(phasesByReceiver[recv], phase)
						} else {
							orphanPhases = append(orphanPhases, phase)
						}
					case "query":
						query := queryDef{Name: attrs["name"], Output: attrs["output"]}
						if query.Name == "" {
							panicError(fmt.Errorf("query on %s is missing name", n.Name.Name))
						}
						if query.Output == "" {
							panicError(fmt.Errorf("query %s is missing output", query.Name))
						}
						if wf := workflowsByType[recv]; wf != nil {
							wf.Queries = append(wf.Queries, query)
						} else {
							orphanQueries = append(orphanQueries, query)
						}
					case "migration":
						from, _ := strconv.Atoi(attrs["from"])
						if from <= 0 {
							panicError(fmt.Errorf("migration on %s is missing from", n.Name.Name))
						}
						if wf := workflowsByType[recv]; wf != nil {
							wf.Migrations = append(wf.Migrations, migrationDef{From: from})
						} else {
							orphanMigrations = append(orphanMigrations, migrationDef{From: from})
						}
					case "signal", "global_signal":
						signal := signalDef{Name: attrs["name"], Payload: attrs["payload"], Global: kind == "global_signal"}
						if signal.Name == "" {
							panicError(fmt.Errorf("%s on %s is missing name", kind, n.Name.Name))
						}
						if wf := workflowsByType[recv]; wf != nil {
							wf.Signals = append(wf.Signals, signal)
						} else {
							orphanSignals = append(orphanSignals, signal)
						}
					case "timer":
						timer := timerDef{Name: attrs["name"], At: attrs["at"]}
						if timer.Name == "" || timer.At == "" {
							panicError(fmt.Errorf("timer on %s is missing name/at", n.Name.Name))
						}
						if wf := workflowsByType[recv]; wf != nil {
							wf.Timers = append(wf.Timers, timer)
						} else {
							orphanTimers = append(orphanTimers, timer)
						}
					case "child":
						child := childDef{Name: attrs["name"], Handle: attrs["handle"]}
						if child.Name == "" || child.Handle == "" {
							panicError(fmt.Errorf("child on %s is missing name/handle", n.Name.Name))
						}
						if wf := workflowsByType[recv]; wf != nil {
							wf.Children = append(wf.Children, child)
						} else {
							orphanChildren = append(orphanChildren, child)
						}
					case "":
					default:
						panicError(fmt.Errorf("unknown durable directive %q on %s", kind, n.Name.Name))
					}
				}
			}
			return true
		})
	}
	var outWorkflows []workflowDef
	for _, wf := range workflowsByType {
		wf.Phases = append(wf.Phases, phasesByReceiver[wf.TypeName]...)
		for _, phase := range orphanPhases {
			wf.Phases = append(wf.Phases, phase)
		}
		wf.Queries = append(wf.Queries, orphanQueries...)
		wf.Migrations = append(wf.Migrations, orphanMigrations...)
		wf.Signals = append(wf.Signals, orphanSignals...)
		wf.Timers = append(wf.Timers, orphanTimers...)
		wf.Children = append(wf.Children, orphanChildren...)
		seen := map[string]string{}
		var phases []phaseDef
		for _, phase := range wf.Phases {
			if phase.Name == "" || phase.StateType == "" {
				continue
			}
			if previous, ok := seen[phase.Name]; ok {
				if previous != "phase" {
					return "", nil, fmt.Errorf("duplicate durable name %q in workflow %s", phase.Name, wf.TypeName)
				}
				return "", nil, fmt.Errorf("duplicate phase name %q in workflow %s", phase.Name, wf.TypeName)
			}
			seen[phase.Name] = "phase"
			phases = append(phases, phase)
		}
		wf.Phases = phases
		for _, item := range namedItems(wf) {
			if previous, ok := seen[item.name]; ok {
				return "", nil, fmt.Errorf("duplicate durable name %q in workflow %s (%s and %s)", item.name, wf.TypeName, previous, item.kind)
			}
			seen[item.name] = item.kind
		}
		outWorkflows = append(outWorkflows, *wf)
	}
	sort.Slice(outWorkflows, func(i, j int) bool { return outWorkflows[i].TypeName < outWorkflows[j].TypeName })
	return pkg.Name, outWorkflows, nil
}

func render(pkg string, workflows []workflowDef) ([]byte, error) {
	var b bytes.Buffer
	fmt.Fprintf(&b, "// Code generated by durable-gen. DO NOT EDIT.\n\n")
	fmt.Fprintf(&b, "package %s\n\n", pkg)
	fmt.Fprintf(&b, "import durable \"github.com/danthegoodman1/durable-phases/go\"\n\n")
	for _, wf := range workflows {
		fmt.Fprintf(&b, "var %sContract = durable.WorkflowContract[%s, %s]{Name: %q, Version: %d}\n\n", wf.TypeName, wf.Input, wf.Output, wf.Name, wf.Version)
		fmt.Fprintf(&b, "type %sPhase interface { is%sPhase() }\n\n", wf.TypeName, wf.TypeName)
		for _, phase := range wf.Phases {
			fmt.Fprintf(&b, "func (%s) is%sPhase() {}\n", phase.StateType, wf.TypeName)
		}
		fmt.Fprintln(&b)
		fmt.Fprintf(&b, "var %sDurableMetadata = struct {\n", wf.TypeName)
		fmt.Fprintf(&b, "\tName string\n\tVersion int\n\tInput string\n\tOutput string\n\tCommon string\n\tPhases []string\n\tQueries []string\n\tMigrations []int\n\tSignals []string\n\tTimers []string\n\tChildren []string\n}{\n")
		fmt.Fprintf(&b, "\tName: %q,\n\tVersion: %d,\n\tInput: %q,\n\tOutput: %q,\n\tCommon: %q,\n", wf.Name, wf.Version, wf.Input, wf.Output, wf.Common)
		fmt.Fprintf(&b, "\tPhases: []string{%s},\n", quotedList(phaseNames(wf.Phases)))
		fmt.Fprintf(&b, "\tQueries: []string{%s},\n", quotedList(queryNames(wf.Queries)))
		fmt.Fprintf(&b, "\tMigrations: []int{%s},\n", intList(migrationVersions(wf.Migrations)))
		fmt.Fprintf(&b, "\tSignals: []string{%s},\n", quotedList(signalNames(wf.Signals)))
		fmt.Fprintf(&b, "\tTimers: []string{%s},\n", quotedList(timerNames(wf.Timers)))
		fmt.Fprintf(&b, "\tChildren: []string{%s},\n", quotedList(childNames(wf.Children)))
		fmt.Fprintf(&b, "}\n\n")
	}
	out, err := format.Source(b.Bytes())
	if err != nil {
		return nil, err
	}
	return out, nil
}

func durableLines(group *ast.CommentGroup) []string {
	var out []string
	for _, comment := range group.List {
		text := strings.TrimSpace(strings.TrimPrefix(comment.Text, "//"))
		if strings.HasPrefix(text, "durable:") {
			out = append(out, strings.TrimPrefix(text, "durable:"))
		}
	}
	return out
}

func parseDirective(line string) (string, map[string]string) {
	parts := strings.Fields(line)
	if len(parts) == 0 {
		return "", nil
	}
	kind := parts[0]
	attrs := map[string]string{}
	for _, part := range parts[1:] {
		if part == "run" {
			attrs["run"] = "true"
			continue
		}
		key, value, ok := strings.Cut(part, "=")
		if ok {
			attrs[key] = strings.Trim(value, `"`)
		}
	}
	return kind, attrs
}

func receiverName(fn *ast.FuncDecl) string {
	if fn.Recv == nil || len(fn.Recv.List) == 0 {
		return ""
	}
	return strings.TrimPrefix(exprString(fn.Recv.List[0].Type), "*")
}

func exprString(expr ast.Expr) string {
	switch value := expr.(type) {
	case *ast.Ident:
		return value.Name
	case *ast.StarExpr:
		return "*" + exprString(value.X)
	case *ast.SelectorExpr:
		return exprString(value.X) + "." + value.Sel.Name
	case *ast.IndexExpr:
		return exprString(value.X) + "[" + exprString(value.Index) + "]"
	case *ast.ArrayType:
		return "[]" + exprString(value.Elt)
	default:
		return fmt.Sprint(reflect.TypeOf(expr))
	}
}

func phaseNames(phases []phaseDef) []string {
	out := make([]string, len(phases))
	for i, phase := range phases {
		out[i] = phase.Name
	}
	sort.Strings(out)
	return out
}

func queryNames(queries []queryDef) []string {
	out := make([]string, len(queries))
	for i, query := range queries {
		out[i] = query.Name
	}
	sort.Strings(out)
	return out
}

func migrationVersions(migrations []migrationDef) []int {
	out := make([]int, len(migrations))
	for i, migration := range migrations {
		out[i] = migration.From
	}
	sort.Ints(out)
	return out
}

func signalNames(signals []signalDef) []string {
	out := make([]string, len(signals))
	for i, signal := range signals {
		out[i] = signal.Name
	}
	sort.Strings(out)
	return out
}

func timerNames(timers []timerDef) []string {
	out := make([]string, len(timers))
	for i, timer := range timers {
		out[i] = timer.Name
	}
	sort.Strings(out)
	return out
}

func childNames(children []childDef) []string {
	out := make([]string, len(children))
	for i, child := range children {
		out[i] = child.Name
	}
	sort.Strings(out)
	return out
}

type namedItem struct {
	name string
	kind string
}

func namedItems(wf *workflowDef) []namedItem {
	var items []namedItem
	for _, query := range wf.Queries {
		items = append(items, namedItem{name: query.Name, kind: "query"})
	}
	for _, signal := range wf.Signals {
		items = append(items, namedItem{name: signal.Name, kind: "signal"})
	}
	for _, timer := range wf.Timers {
		items = append(items, namedItem{name: timer.Name, kind: "timer"})
	}
	for _, child := range wf.Children {
		items = append(items, namedItem{name: child.Name, kind: "child"})
	}
	return items
}

func quotedList(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = strconv.Quote(value)
	}
	return strings.Join(quoted, ", ")
}

func intList(values []int) string {
	out := make([]string, len(values))
	for i, value := range values {
		out[i] = strconv.Itoa(value)
	}
	return strings.Join(out, ", ")
}

func panicError(err error) {
	panic(err)
}
