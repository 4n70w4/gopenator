package main

import (
	"os"
	"io"
	"log"
	"github.com/recursionpharma/go-csv-map"
	"os/exec"
	"time"
	"strconv"
	"net/http"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"strings"
	"io/ioutil"
	"net/url"
	"sync"
	"path/filepath"
	"fmt"
	"github.com/jinzhu/configor"
)



var already = make(map[string] int)


type item struct {
	id string
	reason string
}


type cnt struct {
	lines uint32
	errors uint32
	skipped uint32
	processed uint32
	api_ok uint32
	cli_ok uint32
}


type Config struct {
	Api_url     string `required:"true"`
	Public_key  string `required:"true"`
	Private_key string `required:"true"`
	Location    string `default:"Europe/Moscow"`
}


var counter = cnt{}

var location *time.Location
var config Config



func main() {
	log.Println("Start gopenator")

	initapp()

	go logger()

	log.Println("Open STDIN")
	reader := csvmap.NewReader(os.Stdin)

	processing(*reader)
}



func processing(reader csvmap.Reader) {
	log.Println("Read CSV headers")

	columns, err := reader.ReadHeader()
	reader.Columns = columns

	if err != nil {
		log.Fatal("CSV read columns error: " + err.Error() )
	}

	var wait sync.WaitGroup
	buff_cli := make(chan item, 1)
	buff_api := make(chan item, 1)

	log.Println("Endless reading lines")

	for {
		record, err := reader.Read()
		counter.lines++

		if err == io.EOF {
			log.Println("End of file")
			break
		}

		if err != nil {
			log.Println("CSV read line error: " + err.Error() )
			counter.errors++
		}


		if("inactive" == record["cat"]) {
			if _, exists := already[record["id"]]; exists {
				log.Println("Skip: " + record["id"] )
				counter.skipped++
				continue
			}

			log.Println("Process: " + record["id"] )
			counter.processed++

			already[record["id"]] = 1


			buff_cli <- item{record["id"], record["dsnDiag"]}
			buff_api <- item{record["id"], record["dsnDiag"]}

			wait.Add(2)

			go func(buff_cli chan item) {
				delete(buff_cli)
				wait.Done()
			}(buff_cli)

			go func(buff_api chan item) {
				api_call(buff_api)
				wait.Done()
			}(buff_api)

		}

	}

	wait.Wait()
}



func initapp() {
	filename, _ := filepath.Abs("./gopenator.yaml")
	//	filename, _ := filepath.Abs(filepath.Dir(os.Args[0]) + "/gopenator.yaml")

	configor.New(&configor.Config{Environment: "production"}).Load(&config, filename)
	//	fmt.Printf("config: %#v", config)

	log.Println("Config loaded")


	var err error
	location, err = time.LoadLocation(config.Location)

	if err != nil {
		log.Fatal("Timestamp location: " + err.Error())
	}
}



func logger() {
	log.Println("Start every minute counter logger")

	for {
		nextTime := time.Now().Truncate(time.Minute)
		nextTime = nextTime.Add(time.Minute)
		time.Sleep(time.Until(nextTime))

		println("Counters: " + fmt.Sprintf("%+v", counter) )
	}

}



func delete(buff_cli chan item) {
	item := <- buff_cli

	cmd := exec.Command("mg", "delete", "--id=" + item.id)
	log.Println("Run command:", "mg", "delete", "--id=" + item.id)
	err := cmd.Run()

	if err != nil {
		log.Printf("Command ["+"mg"+"delete"+"--id="+item.id+"] finished with error: %v", err)
	}

}



func get_timestamp() string {
	ts := time.Now().In(location).Unix()
	return strconv.FormatInt(ts, 10)
}



func sign(endpoint string, headers map[string] string, post map[string] string) string {
	form := url.Values{}

	for k, v := range headers {
		form.Add(k, v)
	}

	for k, v := range post {
		form.Add(k, v)
	}

	query := form.Encode()

	separator := "?";
	signatureString := "POST" + " " + endpoint + separator + query;

	key := []byte(config.Private_key)
	h := hmac.New(sha1.New, key)
	h.Write([]byte(signatureString))

	return hex.EncodeToString(h.Sum(nil) )
}



func api_call(buff_api chan item) {
	item := <- buff_api

	headers := make(map[string] string)
	post := make(map[string] string)


	headers["X-MG-PUBLIC-KEY"] = config.Public_key
	headers["X-MG-REMOTE-ADDR"] = ""
	headers["X-MG-TIMESTAMP"] = get_timestamp()

	post["id"] = item.id
	post["reason"] = "Gopenator: " + item.reason

	form := url.Values{}

	for k, v := range post {
		form.Add(k, v)
	}


	endpoint := config.Api_url + "/disable"

	req, err := http.NewRequest("POST", endpoint, strings.NewReader(form.Encode()) )

	if err != nil {
		log.Println("Request prepare error [" + item.id + "]: " + err.Error() )
	}

	req.Header.Add("content-type", "application/x-www-form-urlencoded")
	req.Header.Add("cache-control", "no-cache")


	for k, v := range headers {
		req.Header.Set(k, v)
	}

	signature := sign(endpoint, headers, post)

	req.Header.Set("X-MG-SIGNATURE", signature)

	res, err := http.DefaultClient.Do(req)

	if err != nil {
		log.Println("Request error [" + item.id + "]: " + err.Error() )
	}

	defer res.Body.Close()

	body, _ := ioutil.ReadAll(res.Body)

	log.Println("Api responce [" + item.id + "]: " + string(body) )
}

