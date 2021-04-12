package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

var dbOrders [][]string
var dbProducts [][]string
var key = ""
var lastKey = ""

func printJoin(key string) {
	for _, orderField := range dbOrders {
		for _, productField := range dbProducts {
			fmt.Printf("%s|%s|%s\n", key,
				strings.Join(orderField, "|"),
				strings.Join(productField, "|"))
		}
	}
	dbProducts = dbProducts[:0]
	dbOrders = dbOrders[:0]
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	for {
		line, err := reader.ReadString('\n')
		if len(line) == 0 && err != nil {
			if err == io.EOF {
				break
			}
		}
		if line == "" {
			continue
		}
		line = strings.TrimSuffix(line, "\n")
		lastKey = key
		set := strings.Split(line, "\t")
		key = set[0]
		if lastKey != "" && lastKey != key {
			printJoin(lastKey)
		}
		fields := strings.Split(set[1], ",")
		if len(fields) == 4 {
			dbOrders = append(dbOrders, fields)
		} else {
			dbProducts = append(dbProducts, fields)
		}

	}
	printJoin(lastKey)
}
