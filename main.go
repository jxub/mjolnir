package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"github.com/boltdb/bolt"
)

const (
	localHostPrefix = "127.0.0.1:"
)

type State struct {
	Name      string
	URL       string
	ParentURL string
	DB        *bolt.DB
	Bucket    []byte
	Prefill   map[string]string
	AfterInit bool
}

type Request struct {
	SonURL string
	Value  string
}

type Response struct {
	URL   string
	Value string
}

type Value struct {
	Self   string
	Exists bool
}

func (s *State) Find(reqValue string) (Value, error) {
	v := Value{Self: "", Exists: false}

	if s.DB == nil {
		log.Fatal("db is nil")
	} else if s.Bucket == nil {
		log.Fatal("bucket is nil")
	}

	println(s.DB.Info())

	if err := s.DB.View(func(tx *bolt.Tx) error {
		if tx == nil {
			return fmt.Errorf("tx is nil")
		} else if s == nil {
			return fmt.Errorf("state is nil")
		}
		b := tx.Bucket(s.Bucket)
		val := b.Get([]byte(reqValue))
		if val == nil {
			return fmt.Errorf("key does not exist")
		}
		v.Self = string(val)
		v.Exists = true
		return nil
	}); err != nil {
		return v, err
	}

	return v, nil
}

func (s *State) requestParent(request *Request) (string, error) {
	client := &http.Client{}
	req, err1 := http.NewRequest("GET", "http://"+s.ParentURL, nil)
	if err1 != nil {
		return "", err1
	}

	q := req.URL.Query()
	q.Add("value", request.Value)
	req.URL.RawQuery = q.Encode()

	resp, err2 := client.Do(req)
	if err2 != nil {
		return "", err2
	}

	respBody, err3 := ioutil.ReadAll(resp.Body)

	var response Response
	json.Unmarshal(respBody, &response)

	if err3 != nil {
		return "", err3
	}

	return response.Value, nil
}

func (s *State) requestNode(w http.ResponseWriter, r *http.Request) {

	/*
		if value in node, return value

		else if value from url/get/value not in node,
		call parentURL/get/value and wait (goroutine later)

	*/
	if !s.AfterInit {
		log.Fatal("state was not initialized")
	}

	var request Request
	content, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatal(err)
	}
	json.Unmarshal(content, &request)
	log.Println("requesting value " + request.Value + " from " + request.SonURL)

	reqParams, ok := r.URL.Query()["value"]
	if !ok || len(reqParams[0]) != 1 {
		log.Fatal("request param is missing")
	}

	reqValue := reqParams[0]

	if s.DB == nil {
		log.Fatal("database is not initialized")
	}
	v, err2 := s.Find(reqValue)
	if err2 != nil {
		log.Fatal(err2)
	}
	if v.Self == "" && (s.ParentURL != "") {
		log.Println("requesting value " + reqValue +
			" from parent at " + s.ParentURL)
		newRequest := &Request{s.URL, reqValue}
		value, err3 := s.requestParent(newRequest)

		if err3 != nil {
			log.Fatal(err3)
		}
		if value != "" {
			v.Self = value
			v.Exists = true
		}
	}

	response := Response{URL: s.URL, Value: v.Self}
	log.Println("responding with " + v.Self + " from " + s.URL)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (s *State) Fill() error {
	tx, err := s.DB.Begin(true)
	if err != nil {
		return err
	}

	defer tx.Rollback()
	buck, err2 := tx.CreateBucketIfNotExists([]byte(s.Name))
	if err2 != nil {
		return err2
	}

	s.Bucket = []byte(s.Name)

	for k, v := range s.Prefill {
		if err := buck.Put([]byte(k), []byte(v)); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (s *State) CreateDB(name string) (*bolt.DB, error) {
	db, err := bolt.Open(name+"db", 0600, nil)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func main() {
	name := os.Args[1]
	url := os.Args[2]
	parentURL := os.Args[3]

	url = localHostPrefix + url

	if parentURL == "none" {
		log.Println("Running parent at http://" + url)
		parentURL = ""
	} else {
		parentURL = localHostPrefix + parentURL
		log.Println("Running son at http://" + url +
			" with parent at http://" + parentURL)
	}

	state := NewState(name, url, parentURL)

	db, err := state.Init()
	if err != nil {
		log.Fatal(err)
	}

	defer db.Close()

	http.HandleFunc("/", state.requestNode)
	log.Fatal(http.ListenAndServe(url, nil))
}

func (s *State) Init() (*bolt.DB, error) {
	db, err3 := s.CreateDB(s.Name)
	if err3 != nil {
		return nil, err3
	}

	s.DB = db

	err2 := s.Fill()
	if err2 != nil {
		return nil, err2
	}

	s.AfterInit = true

	return s.DB, nil
}

func NewState(name string, url string, parentURL string) *State {
	dict := map[string]string{}

	if name == "one" {
		dict = map[string]string{"a": "a value", "b": "b value", "c": "c value"}
	} else if name == "two" {
		dict = map[string]string{"a": "a value", "b": "b value"}
	} else if name == "three" {
		dict = map[string]string{"a": "a value"}
	} else {
		panic("name must be in [one, two, three]")
	}

	return &State{
		Name:      name,
		URL:       url,
		ParentURL: parentURL,
		Bucket:    []byte(name),
		DB:        nil,
		Prefill:   dict,
		AfterInit: false,
	}
}
