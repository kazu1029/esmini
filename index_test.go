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
	client, err := New(elastic.SetURL("http://es01:9200"))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Stop()
	ctx := context.TODO()
	_, code, err := client.Ping("http://es01:9200").Do(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if code != 200 {
		t.Errorf("code must be 200, but got %d\n", code)
	}
}

func TestCreate(t *testing.T) {
	client, err := New(elastic.SetURL("http://es01:9200"))
	if err != nil {
		t.Fatal(err)
	}
	index := "tweet"
	defer client.Stop()
	createIndex, err := client.Create(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
	}
	if !createIndex.Acknowledged {
		t.Errorf("expected Acknowledged true, but got false")
	}
	indexExists, err := client.IndexExists(index).Do(context.TODO())
	if err != nil {
		t.Fatal(err)
	}
	if !indexExists {
		t.Errorf("expected index exists, but does not exist")
	}

	client.deleteIndex(index)
}

func TestCreateIndexWithMapping(t *testing.T) {
	index := "tweets"
	client, err := New(elastic.SetURL("http://es01:9200"))
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
		t.Fatal(err)
	}

	if !exists {
		if _, err := client.CreateIndexWithMapping(context.TODO(), index, mapping); err != nil {
			t.Fatal(err)
		}
		exists, err = client.IndexExists(index).Do(context.TODO())
		if err != nil {
			t.Fatal(err)
		}
		if !exists {
			t.Errorf("expected index created, but not created")
		}
	}

	client.deleteIndex(index)
}

func TestCreateTemplate(t *testing.T) {
	client, err := New(elastic.SetURL("http://es01:9200"))
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
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
	client, err := New(elastic.SetURL("http://es01:9200"))
	if err != nil {
		t.Fatal(err)
	}

	index := "tweets"
	defer client.Stop()

	_, err = client.Create(context.TODO(), index)
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}

	res, err := client.MultiGet().
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[0]["index"].Id)).
		Add(elastic.NewMultiGetItem().Index(index).Id(bulkRes.Items[1]["index"].Id)).
		Do(context.TODO())

	if err != nil {
		t.Fatal(err)
	}

	for i, doc := range res.Docs {
		var tw tweet
		if err := json.Unmarshal(doc.Source, &tw); err != nil {
			t.Fatal(err)
		}
		reflect.DeepEqual(tw, tweets[i])
	}

	client.deleteIndex(index)
}

func ExampleNew() {
	urls := []string{"http://es01:9200", "http://es02:9200"}
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
		// elastic client http://es01:9200 returned with code 200 and version 7.4.2
		// elastic client http://es02:9200 returned with code 200 and version 7.4.2
		fmt.Printf("elastic client %s returned with code %d and version %s\n", url, code, info.Version.Number)
	}
}
