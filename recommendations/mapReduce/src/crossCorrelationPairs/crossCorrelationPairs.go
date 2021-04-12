package main

import (
	"log"
	"sort"
	"strings"

	"github.com/vistarmedia/gossamr"
)

type WordCount struct{}

func (wc *WordCount) Map(p int64, line string, c gossamr.Collector) error {
	line = strings.TrimSuffix(line, "\n")
	words := strings.Split(line, " ")
	sort.Strings(words)
	for i, _ := range words {
		for j := i + 1; j < len(words); j++ {
			c.Collect(strings.ToLower(words[i])+" "+strings.ToLower(words[j]), int64(1))
		}
	}
	return nil
}

func (wc *WordCount) Reduce(word string, counts chan int64, c gossamr.Collector) error {
	var sum int64 = 0
	for v := range counts {
		sum += v
	}
	c.Collect(word, sum)
	return nil
}

func main() {
	wordcount := gossamr.NewTask(&WordCount{})

	err := gossamr.Run(wordcount)
	if err != nil {
		log.Fatal(err)
	}
}
