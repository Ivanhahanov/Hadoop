package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {
	var oldTown = ""
	var town = ""
	var townsNames []string
	var auth = 0
	var hub = 0

	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if len(line) == 0 && err != nil {
			if err == io.EOF {
				break
			}
		}
		line = strings.TrimSuffix(line, "\n")
		path := strings.Split(line, "\t")
		oldTown = town
		town = path[0]
		if oldTown != town && oldTown != "" {
			towns := strings.Join(townsNames, ",")
			fmt.Printf("%s\t%s:%d;%d\n", oldTown, towns, auth, hub)
			hub, auth, townsNames = 0, 0, townsNames[:0]
		}
		valuesList := strings.Split(path[1], ":")
		if len(valuesList) > 1 {
			townsNames = append(townsNames, valuesList[0])
			x, err := strconv.Atoi(strings.Split(valuesList[1], ";")[1])
			if err != nil {
				log.Fatal(err)
			}
			auth += x
		} else {
			hub, err = strconv.Atoi(valuesList[0])
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	towns := strings.Join(townsNames, ",")
	fmt.Printf("%s\t%s:%d;%d\n", oldTown, towns, auth, hub)
}
