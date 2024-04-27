package main

import (
	"context"
	"flag"
	"fmt"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os/signal"
	"strings"
	"syscall"
	"worker-template/model"
	"worker-template/processor"
)

func main() {
	var (
		name       string
		configFile string
	)

	flag.StringVar(&name, "n", "", "name processor")
	flag.StringVar(&configFile, "c", "", "name of file config")
	flag.Parse()

	var config model.Config
	configData, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Fatalf("Error reading config file: %v", err)
	}
	err = yaml.Unmarshal(configData, &config)
	if err != nil {
		log.Fatalf("Error parsing YAML: %v\n", err)
	}

	fmt.Println(config)
	contextGlobal, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT, syscall.SIGKILL)
	defer cancel()
	if strings.Contains(name, ",") {
		processors := strings.Split(name, ",")
		for _, name := range processors {
			ps := processor.NewProcessor(contextGlobal, name, &config)
			go ps.Start()
		}
	} else {
		ps := processor.NewProcessor(contextGlobal, name, &config)
		ps.Start()
	}
	select {}

}
