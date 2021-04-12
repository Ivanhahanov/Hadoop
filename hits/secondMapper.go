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
		set := strings.Split(line, "\t")
		town := set[0]
		values := strings.Split(set[1], ":")
		auth := strings.Split(values[1], ";")
		test := strings.Split(values[0], ",")
		for _, value := range test {
			fmt.Printf("%s\t%s:%s\n", value, town, values[1])
		}
		fmt.Printf("%s\t%s\n", town, auth[0])
	}
}
