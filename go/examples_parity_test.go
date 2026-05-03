package durable_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoParityExamplesShowGenerateDX(t *testing.T) {
	examples := []string{
		"immediate-and-signal",
		"timer-stay-restart",
		"stay-loop",
		"child-workflow",
		"migration",
	}
	for _, name := range examples {
		t.Run(name, func(t *testing.T) {
			mainPath := filepath.Join("examples", name, "main.go")
			source, err := os.ReadFile(mainPath)
			if err != nil {
				t.Fatal(err)
			}
			text := string(source)
			for _, want := range []string{
				"//go:generate go run github.com/danthegoodman1/durable-phases/go/cmd/durable-gen",
				"//durable:workflow",
			} {
				if !strings.Contains(text, want) {
					t.Fatalf("%s is missing %q", mainPath, want)
				}
			}
			if strings.Contains(text, "go/internal/demos") {
				t.Fatalf("%s should show the workflow code directly, not call the shared demo runner", mainPath)
			}
			if _, err := os.Stat(filepath.Join("examples", name, "durable_gen.go")); err != nil {
				t.Fatalf("missing generated output for %s: %v", name, err)
			}
		})
	}
}
