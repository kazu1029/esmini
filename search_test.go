package esmini

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

var tweet1 = tweet{Message: "message1", Retweets: 2, Created: time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC), Tags: []string{"tag1", "tag2"}, Category: "Category1"}
var tweet2 = tweet{Message: "message2", Retweets: 5, Created: time.Date(2019, 10, 10, 10, 0, 0, 0, time.UTC), Tags: []string{"tag3", "tag4"}, Category: "Category2"}
var tweet3 = tweet{Message: "message3", Retweets: 0, Created: time.Date(2018, 11, 11, 11, 0, 0, 0, time.UTC), Tags: []string{"tag5", "tag6"}, Category: "Category3"}

func setupTestData(client *elastic.Client, index string) {
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
					},
					"category":{
					  "type":"keyword"
					}
				}
			}
		}
	`
	_, err := client.CreateIndex(index).BodyJson(mapping).Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Index().Index(index).BodyJson(&tweet1).Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet2).Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}
	_, err = client.Index().Index(index).BodyJson(&tweet3).Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	_, err = client.Refresh().Index(index).Do(context.TODO())
	if err != nil {
		log.Fatal(err)
	}

	return
}

func setupTestIterator(t *testing.T) *HitSourceIterator {
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

	iterator := &HitSourceIterator{
		array: []json.RawMessage{json.RawMessage(tw1), json.RawMessage(tw2), json.RawMessage(tw3)},
		index: 0,
	}

	return iterator
}

func TestSearch(t *testing.T) {
	testCases := []struct {
		name   string
		query  string
		fields []string
		tweets []tweet
		opt    []SearchOption
	}{
		{
			"simple", "message", []string{"message"}, []tweet{tweet1, tweet2, tweet3}, nil,
		},
		{
			"with multiple query and multiple fields", "message1 tag6", []string{"message", "tags"}, []tweet{tweet1, tweet3}, []SearchOption{MatchType("most_fields"), Fuzziness("0")},
		},
		{
			"with no match query", "messa", []string{"message"}, []tweet{}, nil,
		},
		{
			"with Limit option", "message", []string{"message"}, []tweet{tweet1}, []SearchOption{Limit(1)},
		},
		{
			"with Limit and From options", "message", []string{"message"}, []tweet{tweet2, tweet3}, []SearchOption{Limit(2), From(1)},
		},
		{
			"with SortField option", "message", []string{"message"}, []tweet{tweet1, tweet3, tweet2}, []SearchOption{SortField("created")},
		},
		{
			"with SortField and Order options", "message", []string{"message"}, []tweet{tweet2, tweet3, tweet1}, []SearchOption{SortField("created"), Order(Desc)},
		},
		{
			"with BoolQueries1", "", []string{"message"}, []tweet{tweet1}, []SearchOption{BoolQueries(map[string]interface{}{"category": "Category1"})},
		},
		{
			"with BoolQueries2", "", []string{"message"}, []tweet{tweet2, tweet3}, []SearchOption{BoolQueries(map[string]interface{}{"category": []string{"Category3", "Category2"}}), BoolClause("should")},
		},
		// {
		// 	"with BoolQueries and Query", "message", []string{"message"}, []tweet{tweet1, tweet2}, []SearchOption{BoolQueries(map[string]interface{}{"category": []string{"Category2", "Category1"}}), BoolClause("must")},
		// },
		{
			"without query and with BoolQueries", "", []string{"message"}, []tweet{tweet1}, []SearchOption{BoolQueries(map[string]interface{}{"category": []string{"Category1", "Category2"}}), Limit(1), From(0), BoolClause("should")},
		},
	}

	index := "tweets"
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	setupTestData(client.raw, index)

	sClient := NewSearchClient(client)

	for _, tt := range testCases {
		t.Run(tt.name, func(t *testing.T) {
			var res SearchResponse
			if tt.opt != nil {
				res, err = sClient.Search(context.TODO(), index, tt.query, tt.fields, tt.opt...)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				res, err = sClient.Search(context.TODO(), index, tt.query, tt.fields)
				if err != nil {
					t.Fatal(err)
				}
			}

			if len(tt.tweets) != int(res.Hits) {
				t.Fatalf("expected %v, but got %v\n", len(tt.tweets), int(res.Hits))
			}

			for j, source := range res.Sources {
				var tw tweet
				_ = json.Unmarshal(source, &tw)

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
				if tt.tweets[j].Category != tw.Category {
					t.Fatalf("expected %v, but got %v\n", tt.tweets[j].Category, tw.Category)
				}
			}
		})
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSearchResultIterator(t *testing.T) {
	iterator := setupTestIterator(t)

	expected := []struct {
		i  int
		tw tweet
	}{
		{0, tweet1},
		{1, tweet2},
		{2, tweet3},
	}

	for iterator.HasNext() {
		var v tweet
		index := iterator.Index()
		err := iterator.Next(&v)
		if err != nil {
			t.Fatal(err)
		}

		if expected[index].i != index {
			fmt.Printf("expected %v, but got %v\n", expected[index].i, index)
		}

		if v.Message != expected[index].tw.Message {
			t.Fatalf("expected %v, but got %v\n", expected[index].tw.Message, v.Message)
		}

		if v.Retweets != expected[index].tw.Retweets {
			t.Fatalf("expected %v, but got %v\n", expected[index].tw.Retweets, v.Retweets)
		}

		if !v.Created.Equal(expected[index].tw.Created) {
			t.Fatalf("expected %v, but got %v\n", expected[index].tw.Created, v.Created)
		}

		if !reflect.DeepEqual(v.Tags, expected[index].tw.Tags) {
			t.Fatalf("expected %v, but got %v\n", expected[index].tw.Tags, v.Tags)
		}

		if v.Category != expected[index].tw.Category {
			t.Fatalf("expected %v, but got %v\n", expected[index].tw.Category, v.Category)
		}
	}
}

func ExampleSearchClient_Search() {
	host := elastic.SetURL(ElasticSearchHost)
	client, err := New(host)
	if err != nil {
		panic(err)
	}

	defer client.Stop()

	index := "example-tweets"
	setupTestData(client.raw, index)

	sClient := NewSearchClient(client)
	ctx := context.Background()
	searchText := "message"
	targetFields := []string{"message", "tags"}

	res, err := sClient.Search(ctx, index, searchText, targetFields)
	if err != nil {
		panic(err)
	}

	itr := res.NewHitSourceIterator()

	// Output:
	// {message1 2 2018-01-02 00:00:00 +0000 UTC [tag1 tag2] Category1}
	// {message2 5 2019-10-10 10:00:00 +0000 UTC [tag3 tag4] Category2}
	// {message3 0 2018-11-11 11:00:00 +0000 UTC [tag5 tag6] Category3}
	for itr.HasNext() {
		var t tweet
		err := itr.Next(&t)
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
