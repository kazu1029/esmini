package esmini

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

var tweet1 = tweet{Message: "message1", Retweets: 2, Created: time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC), Tags: []string{"tag1", "tag2"}}
var tweet2 = tweet{Message: "message2", Retweets: 5, Created: time.Date(2019, 10, 10, 10, 0, 0, 0, time.UTC), Tags: []string{"tag3", "tag4"}}
var tweet3 = tweet{Message: "message3", Retweets: 0, Created: time.Date(2019, 11, 11, 11, 0, 0, 0, time.UTC), Tags: []string{"tag5", "tag6"}}

func setupTestData(t *testing.T, client *elastic.Client) {
	index := "tweets"
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
	_, err := client.CreateIndex(index).BodyJson(mapping).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Index().Index(index).BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	_, err = client.Refresh().Index(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	return
}

func setupIteratorTestData(t *testing.T) *SearchResultIterator {
	tw1, err := json.Marshal(tweet1)
	if err != nil {
		t.Fatal(err)
	}
	tw2, err := json.Marshal(tweet2)
	if err != nil {
		t.Fatal(err)
	}
	tw3, err := json.Marshal(tweet3)
	if err != nil {
		t.Fatal(err)
	}

	iterator := &SearchResultIterator{
		array: []json.RawMessage{json.RawMessage(tw1), json.RawMessage(tw2), json.RawMessage(tw3)},
		index: 3,
	}

	return iterator
}

func TestSearch(t *testing.T) {
	testCases := []struct {
		query  string
		fields []string
		tweets []tweet
	}{
		{
			"message", []string{"message"}, []tweet{tweet1, tweet2, tweet3},
		},
		{
			"message1 tag6", []string{"message", "tags"}, []tweet{tweet1, tweet3},
		},
	}

	index := "tweets"
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	setupTestData(t, client.raw)

	sClient := NewSearch(client)

	for _, tt := range testCases {
		t.Run(tt.query, func(t *testing.T) {
			res, err := sClient.Search(context.TODO(), index, tt.query, tt.fields)
			if err != nil {
				t.Fatal(err)
			}

			for j, row := range res.Results {
				var tw tweet
				_ = json.Unmarshal(row, &tw)

				if tt.tweets[j].Message != tw.Message {
					t.Fatalf("expected %v, but got %v\n", tt.tweets[j].Message, tw.Message)
				}
				if tt.tweets[j].Retweets != tw.Retweets {
					t.Fatalf("expected %v, but got %v\n", tt.tweets[j].Retweets, tw.Retweets)
				}
				if !tt.tweets[j].Created.Equal(tw.Created) {
					t.Fatalf("exected %v, but got %v\n", tt.tweets[j].Created, tw.Created)
				}
				if !reflect.DeepEqual(tt.tweets[j].Tags, tw.Tags) {
					t.Fatalf("expected %v, but got %v\n", tt.tweets[j].Tags, tw.Tags)
				}
			}
		})
	}

	client.deleteIndex(index)
}

func TestIndex(t *testing.T) {
	res := setupIteratorTestData(t)

	expected := []int{0, 1, 2}

	var i int
	for res.HasNext() {
		if expected[i] != res.Index() {
			fmt.Printf("expected %v, but got %v\n", expected[i], res.Index())
		}

		i++
	}
}
