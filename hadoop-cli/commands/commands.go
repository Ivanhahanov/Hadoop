package commands

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
)

type Commands struct {
	NodeAddress string
	NodePort    string
	DirPath     string
	Username    string
}

func (c *Commands) Mkdir(dirName string) error {
	client := &http.Client{}
	directory := ""
	if c.DirPath != "/" {
		directory = c.DirPath
	}

	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, directory, dirName)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	q.Add("op", "MKDIRS")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	var dat map[string]interface{}
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}
	if dat["boolean"] != true {
		return fmt.Errorf("some error")
	}
	return nil
}

type ListOfFiles struct {
	FileStatuses struct {
		FileStatus []struct {
			AccessTime       int    `json:"accessTime"`
			BlockSize        int    `json:"blockSize"`
			ChildrenNum      int    `json:"childrenNum"`
			FileID           int    `json:"fileId"`
			Group            string `json:"group"`
			Length           int    `json:"length"`
			ModificationTime int64  `json:"modificationTime"`
			Owner            string `json:"owner"`
			PathSuffix       string `json:"pathSuffix"`
			Permission       string `json:"permission"`
			Replication      int    `json:"replication"`
			StoragePolicy    int    `json:"storagePolicy"`
			Type             string `json:"type"`
		} `json:"FileStatus"`
	} `json:"FileStatuses"`
}

func (c *Commands) Ls() {

	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1/%s", c.NodeAddress, c.NodePort, c.DirPath)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	q.Add("op", "LISTSTATUS")
	req.URL.RawQuery = q.Encode()
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	var dat ListOfFiles
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}

	for _, file := range dat.FileStatuses.FileStatus {

		fmt.Printf("%s%s  %s  %d  %s\n", string(file.Type[0]), file.Permission, file.Owner, file.Length, file.PathSuffix)
	}
}

func (c *Commands) Put(localFilePath string, localFilename string) error {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, c.DirPath, localFilename)
	req, err := http.NewRequest("PUT", url, nil)
	if err != nil {
		return err
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	//q.Add("overwrite", "true")
	//q.Add("permission", "0666")
	q.Add("noredirect", "true")
	q.Add("op", "CREATE")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	var dat map[string]string
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}
	data, err := os.Open(localFilePath + "/" + localFilename)
	if err != nil {
		return err
	}
	defer data.Close()
	datanode := dat["Location"]
	datanodeUrl := fmt.Sprintf("http://%s:%s", c.NodeAddress, datanode[17:])
	// fmt.Println(datanodeUrl)
	req, err = http.NewRequest("PUT", datanodeUrl, data)
	if err != nil {
		return err
	}

	client = &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	return nil

} //<имя локального файла> (загрузка файла в HDFS);
func (c *Commands) Get(remoteFilePath string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, c.DirPath, remoteFilePath)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
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
	fmt.Println(string(body))
} //<имя файла в HDFS> (скачивание файла из HDFS);
func (c *Commands) Append(localFilePath string, remoteFilename string) error {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, c.DirPath, remoteFilename)
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		return nil
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	q.Add("noredirect", "true")
	q.Add("op", "APPEND")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	var dat map[string]string
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}

	data, err := os.Open(localFilePath)
	if err != nil {
		return err
	}
	defer data.Close()
	datanode := dat["Location"]
	datanodeUrl := fmt.Sprintf("http://%s:%s", c.NodeAddress, datanode[17:])

	req, err = http.NewRequest("POST", datanodeUrl, data)
	if err != nil {
		return err
	}
	client = &http.Client{}
	resp, err = client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ = ioutil.ReadAll(resp.Body)
	return nil
} //<имя локального файла> <имя файла в HDFS> (конкатенация файла в HDFS с локальным файлом);
func (c *Commands) Delete(remoteFilePath string) error {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s:%s/webhdfs/v1%s/%s", c.NodeAddress, c.NodePort, c.DirPath, remoteFilePath)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Print(err)
		os.Exit(1)
	}

	q := req.URL.Query()
	q.Add("user.name", c.Username)
	q.Add("op", "DELETE")
	req.URL.RawQuery = q.Encode()

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}

	var dat map[string]interface{}
	body, _ := ioutil.ReadAll(resp.Body)
	if err := json.Unmarshal(body, &dat); err != nil {
		panic(err)
	}
	if dat["boolean"] != true {
		return fmt.Errorf("some error")
	}
	return nil
}
