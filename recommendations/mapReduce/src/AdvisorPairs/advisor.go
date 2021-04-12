package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
)

type Commands struct {
	NodeAddress string
	NodePort    string
	DirPath     string
	Username    string
}

var c = Commands{
	NodeAddress: "192.168.0.156",
	NodePort:    "50070",
	DirPath:     "/crossCorrelationPairs",
	Username:    "vagrant",
}

func Get(remoteFilePath string) (string, error) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, c.DirPath, remoteFilePath)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	q.Add("noredirect", "true")
	q.Add("op", "OPEN")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	var dat map[string]string
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}

	datanode := dat["Location"]
	datanodeUrl := fmt.Sprintf("http://%s:%s", c.NodeAddress, datanode[17:])

	resp, err = http.Get(datanodeUrl)
	if err != nil {
		log.Fatal(err)
	}

	body, _ = ioutil.ReadAll(resp.Body)
	return string(body), nil
}

type Line struct {
	Count int
	Word  string
	Pair  string
}

type Pair struct {
	Count   int
	Product string
}

func main() {
	data, err := Get("part-00000")
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("word -> ")
	word, err := reader.ReadString('\n')
	word = strings.TrimSuffix(word, "\n")
	var products []*Line
	for _, line := range strings.Split(data, "\n") {
		if line == "" {
			break
		}
		splitLine := strings.Split(line, "\t")
		count, _ := strconv.Atoi(splitLine[0])
		arrWord := strings.Split(splitLine[1], " ")
		products = append(products, &Line{Count: count, Word: arrWord[0], Pair: arrWord[1]})
	}
	var pairs []*Pair
	for _, product := range products {

		if product.Word == word {
			pairs = append(pairs, &Pair{
				Count:   product.Count,
				Product: product.Pair,
			})
		} else if product.Pair == word {
			pairs = append(pairs, &Pair{
				Count:   product.Count,
				Product: product.Word,
			})
		}
	}
	sort.Slice(pairs, func(i, j int) bool {
		return pairs[i].Count > pairs[j].Count
	})
	for i, pair := range pairs {
		if i >= 10 {
			break
		}
		fmt.Println(pair.Count, pair.Product)
	}
}
