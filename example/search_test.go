package esmini

import (
	"context"
	"esmini"
	"fmt"
	"time"

	"github.com/olivere/elastic/v7"
)

type tweet struct {
	Message  string    `json:"message"`
	Retweets int       `json:"retweets"`
	Created  time.Time `json:"created,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
}

var tweet1 = tweet{Message: "message1", Retweets: 2, Created: time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC), Tags: []string{"tag1", "tag2"}}
var tweet2 = tweet{Message: "message2", Retweets: 5, Created: time.Date(2019, 10, 10, 10, 0, 0, 0, time.UTC), Tags: []string{"tag3", "tag4"}}
var tweet3 = tweet{Message: "message3", Retweets: 0, Created: time.Date(2018, 11, 11, 11, 0, 0, 0, time.UTC), Tags: []string{"tag5", "tag6"}}

func setupTestData(host elastic.ClientOptionFunc) {
	client, err := elastic.NewClient(host)
	if err != nil {
		panic(err)
	}

	defer client.Stop()

	index := "example-tweets"
	mapping := `
	  {
			"settings":{
			  "number_of_shards":1,
				"number_of_replicas":0
			},
			"mappings":{
				"properties":{
				  "message":{
					  "type":"text"
					},
					"created":{
					  "type":"date"
					},
					"tags":{
					  "type":"text"
					}
				}
			}
		}
	`

	_, err = client.CreateIndex(index).BodyJson(mapping).Do(context.TODO())
	if err != nil {
		panic(err)
	}

	_, err = client.Index().Index(index).BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		panic(err)
	}

	_, err = client.Refresh().Index(index).Do(context.TODO())
	if err != nil {
		panic(err)
	}
}

func ExampleSearchClient_Search() {
	host := elastic.SetURL(ElasticSearchHost1)
	client, err := esmini.New(host)
	if err != nil {
		panic(err)
	}

	defer client.Stop()

	setupTestData(host)

	sClient := esmini.NewSearchClient(client)
	ctx := context.Background()
	index := "example-tweets"
	searchText := "message"
	targetFields := []string{"message", "tags"}

	res, err := sClient.Search(ctx, index, searchText, targetFields)
	if err != nil {
		panic(err)
	}

	// Output:
	// {message1 2 2018-01-02 00:00:00 +0000 UTC [tag1 tag2]}
	// {message2 5 2019-10-10 10:00:00 +0000 UTC [tag3 tag4]}
	// {message3 0 2018-11-11 11:00:00 +0000 UTC [tag5 tag6]}
	for res.HasNext() {
		var t tweet
		err := res.Next(&t)
		if err != nil {
			return
		}

		fmt.Printf("%v\n", t)
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		panic(err)
	}
}
