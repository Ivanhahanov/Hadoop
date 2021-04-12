package main

import (
	"encoding/json"
	"log"
	"sort"
	"strings"

	"github.com/vistarmedia/gossamr"
)

type WordCount struct{}

func (wc *WordCount) Map(p int64, line string, c gossamr.Collector) error {
	line = strings.TrimSuffix(line, "\n")
	wordsArr := strings.Split(line, " ")
	sort.Strings(wordsArr)
	for _, word := range wordsArr {
		stripe := make(map[string]int64)
		for _, pair := range wordsArr {
			if word != pair {
				stripe[pair] += 1
			}
		}
		jsonString, _ := json.Marshal(stripe)
		c.Collect(word, string(jsonString))
	}
	return nil
}

func (wc *WordCount) Reduce(word string, counts chan string, c gossamr.Collector) error {
	finalStripe := make(map[string]int64)
	for str := range counts {
		stripe := make(map[string]int64)
		json.Unmarshal([]byte(str), &stripe)
		for key, val := range stripe {
			finalStripe[key] += val
		}
	}
	jsonString, _ := json.Marshal(finalStripe)
	c.Collect(word, string(jsonString))
	return nil
}

func main() {
	wordcount := gossamr.NewTask(&WordCount{})

	err := gossamr.Run(wordcount)
	if err != nil {
		log.Fatal(err)
	}
}
