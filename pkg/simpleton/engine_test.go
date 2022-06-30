package simpleton_test

import (
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"

	"github.com/mraufc/simpleton/pkg/simpleton"
)

func TestEngine_Get(t *testing.T) {
	type args struct {
		key string
	}
	tests := []struct {
		name    string
		e       *simpleton.Engine
		args    args
		want    string
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.e.Get(tt.args.key)
			if (err != nil) != tt.wantErr {
				t.Errorf("Engine.Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Engine.Get() = %v, want %v", got, tt.want)
			}
		})
	}
}

func ExampleNewEngine() {
	f, err := ioutil.TempFile("./", "simpleton-")
	if err != nil {
		log.Fatalln(err)
	}
	name := f.Name()
	defer os.Remove(name)
	defer f.Close()
	e, err := simpleton.NewEngine(f, nil, nil)
	if err != nil {
		log.Fatalln(err)
	}
	key, value := "example_key", "example_value"
	_, err = e.Get(key)
	fmt.Println(err)
	if err := e.Put(key, value); err != nil {
		log.Fatalln(err)
	}
	v, err := e.Get(key)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("value retrieved from database is correct: %v", v == value)

	// Output:
	// key not found example_key
	// value retrieved from database is correct: true
}

func ExampleNewEngine_reopen() {
	f, err := ioutil.TempFile("./", "simpleton-")
	if err != nil {
		log.Fatalln(err)
	}
	name := f.Name()
	defer os.Remove(name)
	e, err := simpleton.NewEngine(f, nil, nil)
	if err != nil {
		log.Fatalln(err)
	}
	key, value := "example_key", "example_value"
	_, err = e.Get(key)
	fmt.Println(err)
	if err := e.Put(key, value); err != nil {
		log.Fatalln(err)
	}
	v, err := e.Get(key)
	if err != nil {
		log.Fatalln(err)
	}
	fmt.Printf("value retrieved from database is correct: %v\n", v == value)

	f.Close()
	f2, err := os.OpenFile(name, os.O_RDWR, 0644)
	if err != nil {
		log.Fatalln(err)
	}
	defer f2.Close()
	e2, err := simpleton.NewEngine(f2, nil, nil)
	if err != nil {
		log.Fatalln(err)
	}
	v2, err := e2.Get(key)
	if err != nil {
		log.Fatalln(err)
	}

	fmt.Printf("value retrieved from re-opened database is correct: %v\n", v2 == value)

	// Output:
	// key not found example_key
	// value retrieved from database is correct: true
	// value retrieved from re-opened database is correct: true
}
