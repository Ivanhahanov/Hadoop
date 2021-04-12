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
	DirPath:     "/crossCorrelationStripes",
	Username:    "vagrant",
}

type Pair struct {
	Key   string
	Value int
}

type PairList []Pair

func (p PairList) Len() int           { return len(p) }
func (p PairList) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }
func (p PairList) Less(i, j int) bool { return p[i].Value > p[j].Value }

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

func main() {
	data, err := Get("part-00000")
	if err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Printf("word -> ")
	word, err := reader.ReadString('\n')
	word = strings.TrimSuffix(word, "\n")

	var stripe map[string]int

	for _, line := range strings.Split(data, "\n") {
		if line == "" {
			break
		}
		splitLine := strings.Split(line, "\t")
		if splitLine[0] == word {
			json.Unmarshal([]byte(splitLine[1]), &stripe)
		}
	}
	p := make(PairList, len(stripe))

	i := 0
	for k, v := range stripe {
		p[i] = Pair{k, v}
		i++
	}

	sort.Sort(p)

	for _, k := range p {
		fmt.Printf("%v %v\n", k.Value, k.Key)
	}

}
