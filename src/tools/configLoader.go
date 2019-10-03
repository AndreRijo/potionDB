package tools

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
)

type ConfigLoader struct {
	configs map[string]string
	folder  string
}

const (
	//baseFilePath = "../../configs/"
	configType = ".cfg"
)

var (
	SharedConfig *ConfigLoader
)

func (config *ConfigLoader) LoadConfigs(folder string) {
	SharedConfig = config
	config.configs = make(map[string]string)
	if folder[len(folder)-1] != '/' {
		//config.folder = baseFilePath + folder + "/"
		config.folder = folder + "/"
	} else {
		//config.folder = baseFilePath + folder
		config.folder = folder
	}
	filesToRead := config.getConfigFiles()
	for _, filePath := range filesToRead {
		config.readConfigFile(filePath)
	}
	fmt.Println("Finished reading configs")
}

func (config *ConfigLoader) GetConfig(key string) (value string) {
	return config.configs[key]
}

func (config *ConfigLoader) GetAndHasConfig(key string) (value string, has bool) {
	value, has = config.configs[key]
	return
}

func (config *ConfigLoader) getConfigFiles() (fileNames []string) {
	files, _ := ioutil.ReadDir(config.folder)
	fileNames = make([]string, 0, 10)
	for _, file := range files {
		if strings.HasSuffix(file.Name(), configType) {
			fileNames = append(fileNames, config.folder+file.Name())
		}
	}
	return
}

func (config *ConfigLoader) readConfigFile(fileName string) {
	file, _ := os.Open(fileName)
	defer file.Close()
	in := bufio.NewReader(file)
	var err error
	var str string
	for err == nil {
		str, err = in.ReadString('\n')
		if err == nil && !config.isComment(str) {
			parts := strings.Split(str, "=")
			//fmt.Println("Read:", parts)
			config.configs[strings.TrimSpace(parts[0])] = strings.TrimSpace(parts[1])
			//fmt.Println(strings.TrimSpace(parts[0]) + "=" + strings.TrimSpace(parts[1]))
		}
	}
}

func (config *ConfigLoader) isComment(line string) (isComment bool) {
	return line == "\n" || strings.HasPrefix("//", line) || line[0] == '#'
}
