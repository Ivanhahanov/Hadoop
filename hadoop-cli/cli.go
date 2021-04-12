package main

import (
	"Hadoop/hadoop-cli/commands"
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

var c = commands.Commands{
	DirPath:  "/",
	Username: "vagrant",
}
var localPath, _ = os.Getwd()

func checkConnection() error {
	if _, err := net.DialTimeout("tcp", "google.com:80", 1*time.Second); err != nil {
		return fmt.Errorf("no connetcion to internet")
	}

	nameNode := fmt.Sprintf("%s:%s", c.NodeAddress, c.NodePort)
	if _, err := net.DialTimeout("tcp", nameNode, 1*time.Second); err != nil {
		return err
	}
	return nil
}

func printHelp(command string) {
	switch command {
	case "append":
		fmt.Println("append [local filename] [remote filename]")
		fmt.Printf("NOTE: before execute command %s use cd and lcd\n", command)
	}
}

func main() {
	c.NodeAddress = os.Args[1]
	c.NodePort = os.Args[2]
	if err := checkConnection(); err != nil {
		log.Fatal(err)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("[%s@%s %s]$ ", c.Username, "hadoop", c.DirPath)
		cmdString, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
		err = runCommand(cmdString)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}
}
func runCommand(commandStr string) error {
	commandStr = strings.TrimSuffix(commandStr, "\n")
	arrCommandStr := strings.Fields(commandStr)
	switch arrCommandStr[0] {
	case "exit":
		os.Exit(0)
	case "mkdir":
		c.Mkdir(arrCommandStr[1])
	case "put":
		if err := c.Put(localPath, arrCommandStr[1]); err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("%s upload successfuly\n", arrCommandStr[1])
		}
	case "get":
		c.Get(arrCommandStr[1])
	case "append":
		if len(arrCommandStr) == 3 {
			if err := c.Append(localPath+"/"+arrCommandStr[1], arrCommandStr[2]); err != nil {
				fmt.Println(err)
			}
		} else {
			printHelp(arrCommandStr[0])
		}
	case "ls":
		c.Ls()
	case "rm":
		if err := c.Delete(arrCommandStr[1]); err != nil {
			fmt.Println(err)
		} else {
			fmt.Printf("%s deleted successfuly\n", arrCommandStr[1])
		}
	case "cd":
		switch arrCommandStr[1] {
		case "..":
			directories := strings.Split(c.DirPath, "/")
			c.DirPath = strings.Join(directories[:len(directories)-1], "/")
			if c.DirPath == "" {
				c.DirPath = "/"
			}

		case "/":
			c.DirPath = "/"
		default:
			if c.DirPath == "/" {
				c.DirPath += arrCommandStr[1]
			} else {
				c.DirPath = c.DirPath + "/" + arrCommandStr[1]
			}
		}
	case "lls":
		files, err := ioutil.ReadDir(localPath)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(localPath)
		for _, f := range files {
			fmt.Println(f.Name())
		}
	case "lcd":
		switch arrCommandStr[1] {
		case "..":
			directories := strings.Split(localPath, "/")
			localPath = strings.Join(directories[:len(directories)-1], "/")
			fmt.Println(localPath)
		case "/":
			localPath = "/"
		default:
			newPath := localPath + "/" + arrCommandStr[1]

			if _, err := os.Stat(newPath); os.IsNotExist(err) {
				fmt.Printf("Error: directory %s does not exist\n", newPath)
			}
			localPath = newPath
		}
	}
	return nil
}
