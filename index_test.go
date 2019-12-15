package esmini

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/olivere/elastic/v7"
)

const ES_HOST = "http://es01:9200"

func (i *IndexClient) deleteIndex(index string) {
	_, err := i.DeleteIndex(index).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	i.Stop()
}

func (i *IndexClient) deleteTemplate(tempName string) {
	_, err := i.IndexDeleteTemplate(tempName).Do(context.TODO())
	if err != nil {
		panic(err)
	}
	i.Stop()
}

func TestNew(t *testing.T) {
	client, err := New(elastic.SetURL(ES_HOST))
	if err != nil {
		t.Fatalf(err.Error())
	}
	defer client.Stop()
	ctx := context.TODO()
	_, code, err := client.Ping(ES_HOST).Do(ctx)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if code != 200 {
		t.Fatalf("code must be 200, but got %d\n", code)
	}
}

func TestCreate(t *testing.T) {
	client, err := New(elastic.SetURL(ES_HOST))
	if err != nil {
		t.Fatalf(err.Error())
	}
	index := "tweet"
	defer client.Stop()
	createIndex, err := client.Create(context.TODO(), index)
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !createIndex.Acknowledged {
		t.Errorf("expected Acknowledged true, but got false")
	}
	indexExists, err := client.IndexExists(index).Do(context.TODO())
	if err != nil {
		t.Fatalf(err.Error())
	}
	if !indexExists {
		t.Errorf("expected index exists, but does not exist")
	}

	client.deleteIndex(index)
}

func TestCreateIndexWithMapping(t *testing.T) {
	index := "tweets"
	client, err := New(elastic.SetURL(ES_HOST))
	if err != nil {
		t.Fatalf(err.Error())
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
          "user":{
            "type":"keyword"
          },
          "message":{
            "type":"text",
            "store": true,
            "fielddata": true
          },
          "image":{
            "type":"keyword"
          },
          "created":{
            "type":"date"
          },
          "tags":{
            "type":"keyword"
          },
          "location":{
            "type":"geo_point"
          }
        }
      }
    }
  }`
	exists, err := client.IndexExists(index).Do(context.TODO())
	if err != nil {
		t.Fatalf(err.Error())
	}

	if !exists {
		if _, err := client.CreateIndexWithMapping(context.TODO(), index, mapping); err != nil {
			t.Fatalf(err.Error())
		}
		exists, err = client.IndexExists(index).Do(context.TODO())
		if err != nil {
			t.Fatalf(err.Error())
		}
		if !exists {
			t.Fatalf("expected index created, but not created")
		}
	}

	client.deleteIndex(index)
}

func TestCreateTemplate(t *testing.T) {
	client, err := New(elastic.SetURL(ES_HOST))
	if err != nil {
		t.Fatalf(err.Error())
	}
	tempName := "tweet-template"
	defer client.deleteTemplate(tempName)

	const template = `
  {
    "index_patterns": ["tweet*"],
    "settings": {
      "number_of_shards": 1,
      "number_of_replicas": 0
    },
    "mappings": {
      "properties": {
        "user":{
          "type":"keyword"
       },
       "message":{
         "type":"text",
         "store": true,
         "fielddata": true
        },
        "image":{
          "type":"keyword"
        },
        "created":{
          "type":"date"
        },
        "tags":{
          "type":"keyword"
        },
        "location":{
          "type":"geo_point"
        }
      }
    }
  }
  `

	if _, err := client.CreateTemplate(context.TODO(), tempName, template); err != nil {
		t.Fatalf(err.Error())
	}
}

type tweet struct {
	User     string    `json:"user"`
	Message  string    `json:"message"`
	Retweets int       `json:"retweets"`
	Image    string    `json:"image,omitempty"`
	Created  time.Time `json:"created,omitempty"`
	Tags     []string  `json:"tags,omitempty"`
	Location string    `json:"location,omitempty"`
}

func TestBulkInsert(t *testing.T) {
	client, err := New(elastic.SetURL(ES_HOST))
	if err != nil {
		t.Fatalf(err.Error())
	}

	index := "tweets"
	defer client.Stop()

	_, err = client.Create(context.TODO(), index)
	if err != nil {
		t.Fatalf(err.Error())
	}

	tweets := []tweet{
		tweet{User: "user1", Message: "message1", Retweets: 1, Image: "image1", Created: time.Now(), Tags: []string{"tag1", "tag2"}, Location: "Tokyo"},
		tweet{User: "user2", Message: "message2", Retweets: 2, Image: "image2", Created: time.Now(), Tags: []string{"tag1", "tag2"}, Location: "Tokyo"},
	}

	docs := make([]interface{}, len(tweets))
	for i, v := range tweets {
		docs[i] = v
	}

	bulkRes, err := client.BulkInsert(context.TODO(), index, docs)
	if err != nil {
		t.Fatalf(err.Error())
	}

	res, err := client.MultiGet().
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[0]["index"].Id)).
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[1]["index"].Id)).
		Do(context.TODO())

	if err != nil {
		t.Fatalf(err.Error())
	}

	for i, doc := range res.Docs {
		var tw tweet
		if err := json.Unmarshal(doc.Source, &tw); err != nil {
			t.Fatalf(err.Error())
		}
		reflect.DeepEqual(tw, tweets[i])
	}

	client.deleteIndex(index)
}
