package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if len(line) == 0 && err != nil {
			if err == io.EOF {
				break
			}
		}
		line = strings.TrimSuffix(line, "\n")
		set := strings.Split(line, ",")
		fmt.Printf("%s\t%s\n", set[0], strings.Join(set[1:], ","))
	}
}
