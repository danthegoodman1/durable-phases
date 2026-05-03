package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
)

var examples = []string{
	"immediate-and-signal",
	"timer-stay-restart",
	"stay-loop",
	"child-workflow",
	"migration",
}

func main() {
	name := "immediate-and-signal"
	if len(os.Args) > 1 {
		name = os.Args[1]
	}
	var err error
	if name == "all" || name == "index" {
		for index, example := range examples {
			if index > 0 {
				fmt.Println()
			}
			fmt.Printf("== %s ==\n", example)
			if err = run(example); err != nil {
				break
			}
		}
	} else {
		err = run(name)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n\navailable demos: %s\n", err, strings.Join(examples, ", "))
		os.Exit(1)
	}
}

func run(name string) error {
	found := false
	for _, example := range examples {
		if example == name {
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("unknown demo %q", name)
	}
	cmd := exec.Command("go", "run", "./examples/"+name)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}
