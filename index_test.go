package esmini

import (
	"container/list"
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

const ElasticSearchHost = "http://es01:9200"

func (i *IndexClient) deleteTemplate(tempName string) {
	_, err := i.raw.IndexDeleteTemplate(tempName).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	i.Stop()
}

func TestNew(t *testing.T) {
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	ctx := context.TODO()
	_, code, err := client.Ping(ctx, ElasticSearchHost)
	if err != nil {
		t.Fatal(err)
	}
	if code != 200 {
		t.Fatalf("code must be 200, but got %d\n", code)
	}
}

func TestCreateIndex(t *testing.T) {
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	index := "tweet"
	defer client.Stop()

	createIndex, err := client.CreateIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
	if !createIndex.Acknowledged {
		t.Errorf("expected Acknowledged true, but got false")
	}
	indexExists, err := client.raw.IndexExists(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if !indexExists {
		t.Error("expected index exists, but does not exist")
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateIndexWithMapping(t *testing.T) {
	index := "tweets"
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()

	mapping := `
    {
      "settings":{
        "number_of_shards": 1,
        "number_of_replicas": 0
      },
      "mappings":{
        "properties":{
          "foo":{
            "type":"keyword"
          }
				}
      }
    }
  }`
	exists, err := client.raw.IndexExists(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	if !exists {
		if _, err := client.CreateIndexWithMapping(context.TODO(), index, mapping); err != nil {
			t.Fatal(err)
		}
		exists, err = client.raw.IndexExists(index).Do(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Fatal("expected index created, but not created")
		}
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
}

func TestCreateTemplate(t *testing.T) {
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}
	tempName := "tweet-template"
	defer client.deleteTemplate(tempName)

	const template = `
  {
    "index_patterns": ["foo*"],
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "name":{
          "type":"keyword"
        }
      }
    }
  }
  `

	if _, err := client.CreateTemplate(context.TODO(), tempName, template); err != nil {
		t.Fatal(err)
	}
}

type tweet struct {
	Message  string    `json:"message"`
	Retweets int       `json:"retweets"`
	Created  time.Time `json:"created,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Category string    `json:"category,omitempty"`
}

func TestBulkInsert(t *testing.T) {
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}

	index := "tweets"
	defer client.Stop()

	_, err = client.raw.CreateIndex(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	tweets := list.New()
	tweets.PushBack(tweet{Message: "message1", Retweets: 1, Created: time.Now(), Tags: []string{"tag1", "tag2"}})
	tweets.PushBack(tweet{Message: "message2", Retweets: 2, Created: time.Now(), Tags: []string{"tag1", "tag2"}})

	bulkRes, err := client.BulkInsert(context.TODO(), index, tweets)
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.raw.MultiGet().
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[0]["index"].Id)).
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[1]["index"].Id)).
		Do(context.TODO())

	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for e := tweets.Front(); e != nil; e = e.Next() {
		var tw tweet
		if err := json.Unmarshal(res.Docs[i].Source, &tw); err != nil {
			t.Fatal(err)
		}
		expected, ok := e.Value.(tweet)
		if !ok {
			t.Fatalf("expected %v, but got %v\n", reflect.TypeOf(expected), reflect.TypeOf(tw))
		}
		if expected.Message != tw.Message {
			t.Fatalf("expected %v, but got %v\n", expected.Message, tw.Message)
		}
		if expected.Retweets != tw.Retweets {
			t.Fatalf("expected %v, but got %v\n", expected.Retweets, tw.Retweets)
		}
		if !expected.Created.Equal(tw.Created) {
			t.Fatalf("expected %v, but got %v\n", expected.Created, tw.Created)
		}
		if !reflect.DeepEqual(expected.Tags, tw.Tags) {
			t.Fatalf("expected %v, but got %v\n", expected.Tags, tw.Tags)
		}
		i++
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
}

type tweetWithID struct {
	ID       int       `json:"id"`
	Message  string    `json:"message"`
	Retweets int       `json:"retweets"`
	Created  time.Time `json:"created,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Category string    `json:"category,omitempty"`
}

func TestBulkInsertWithOptions(t *testing.T) {
	client, err := New(elastic.SetURL(ElasticSearchHost))
	if err != nil {
		t.Fatal(err)
	}

	index := "tweets_with_id"
	defer client.Stop()

	_, err = client.raw.CreateIndex(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}

	tweets := list.New()
	tweets.PushBack(tweetWithID{ID: 1, Message: "message1", Retweets: 1, Created: time.Now(), Tags: []string{"tag1", "tag2"}})
	tweets.PushBack(tweetWithID{ID: 2, Message: "message2", Retweets: 2, Created: time.Now(), Tags: []string{"tag1", "tag2"}})

	bulkRes, err := client.BulkInsert(context.TODO(), index, tweets, DocID("ID"))
	if err != nil {
		t.Fatal(err)
	}

	res, err := client.raw.MultiGet().
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[0]["index"].Id)).
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[1]["index"].Id)).
		Do(context.TODO())

	if err != nil {
		t.Fatal(err)
	}

	i := 0
	for e := tweets.Front(); e != nil; e = e.Next() {
		var tw tweetWithID
		if err := json.Unmarshal(res.Docs[i].Source, &tw); err != nil {
			t.Fatal(err)
		}
		expected, ok := e.Value.(tweetWithID)
		if !ok {
			t.Fatalf("expected %v, but got %v\n", reflect.TypeOf(expected), reflect.TypeOf(tw))
		}
		if expected.Message != tw.Message {
			t.Fatalf("expected %v, but got %v\n", expected.Message, tw.Message)
		}
		if expected.Retweets != tw.Retweets {
			t.Fatalf("expected %v, but got %v\n", expected.Retweets, tw.Retweets)
		}
		if !expected.Created.Equal(tw.Created) {
			t.Fatalf("expected %v, but got %v\n", expected.Created, tw.Created)
		}
		if !reflect.DeepEqual(expected.Tags, tw.Tags) {
			t.Fatalf("expected %v, but got %v\n", expected.Tags, tw.Tags)
		}
		i++
	}

	_, err = client.DeleteIndex(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
}
