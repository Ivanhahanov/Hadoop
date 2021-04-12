package main

import (
	"encoding/json"
	"fmt"
	"strings"
)

func main() {
	allWords := "арбуз банан сок\nбанан сникерс\nсникерс дыня\nбанан сок дыня\nхлеб соль"
	mappedWords := ""
	for _, words := range strings.Split(allWords, "\n") {
		wordsArr := strings.Split(words, " ")
		for _, word := range wordsArr {
			stripe := make(map[string]int64)
			for _, pair := range wordsArr {
				if word != pair {
					stripe[pair] += 1
				}
			}
			jsonString, _ := json.Marshal(stripe)
			mappedWords += fmt.Sprintln(word, string(jsonString))
		}
	}
	fmt.Print(mappedWords)
}
