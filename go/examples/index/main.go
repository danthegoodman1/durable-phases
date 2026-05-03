package main

import (
	"fmt"
	"os"
	"os/exec"
)

var examples = []string{
	"immediate-and-signal",
	"timer-stay-restart",
	"stay-loop",
	"child-workflow",
	"migration",
}

func main() {
	for index, name := range examples {
		if index > 0 {
			fmt.Println()
		}
		fmt.Printf("== %s ==\n", name)
		cmd := exec.Command("go", "run", "./examples/"+name)
		cmd.Stdout = os.Stdout
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	}
}
