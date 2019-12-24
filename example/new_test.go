package esmini

import (
	"context"
	"esmini"
	"fmt"

	"github.com/olivere/elastic/v7"
)

const (
	ElasticSearchHost1 = "http://es01:9200"
	ElasticSearchHost2 = "http://es02:9200"
)

func ExampleNew() {
	urls := []string{ElasticSearchHost1, ElasticSearchHost2}
	client, err := esmini.New(
		elastic.SetURL(urls...),
	)

	if err != nil {
		panic(err)
	}

	defer client.Stop()

	for _, url := range urls {
		ctx := context.Background()
		info, code, err := client.Ping(ctx, url)

		if err != nil {
			panic(err)
		}

		// Output:
		// elastic client http://es01:9200 returned with code 200 and version 7.4.2
		// elastic client http://es02:9200 returned with code 200 and version 7.4.2
		fmt.Printf("elastic client %s returned with code %d and version %s\n", url, code, info.Version.Number)
	}
}
