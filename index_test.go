package esmini

import (
	"context"
	"fmt"
	"testing"

	"github.com/olivere/elastic"
)

func TestNew(t *testing.T) {
	client, err := New()
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	fmt.Printf("client: %v\n", client)
}

func ExampleNew() {
	urls := []string{"http://192.168.2.11:9200", "http://192.168.2.10:9201"}
	client, err := New(
		elastic.SetURL(urls...),
	)

	if err != nil {
		panic(err)
	}

	defer client.Stop()

	for _, url := range urls {
		ctx := context.Background()
		info, code, err := client.Ping(url).Do(ctx)

		if err != nil {
			panic(err)
		}

		// Output:
		// elastic client returned with code 200 and version ~~~
		fmt.Printf("elastic client returned with code %d and version %s\n", code, info.Version.Number)
	}
}
