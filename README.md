# esmini

**esmini** is a minimum library for `olivere/elastic/v7`.

## Installation

`go get -u github.com/kazu1029/esmini`

## Quick Start

See the `examples/*` or `*_test.go` for sample implementation.

The following example is searching from tweets and iterating each tweets.

```
const ElasticSearchHost1 = "http://es01:9200"
const ElasticSearchHost2 = "http://es02:9200"
urls := []string{ElasticSearchHost1, ElasticSearchHost2}
client, err := esmini.New(
	elastic.SetURL(urls...),
)
if err != nil {
    fmt.Printf("Error: %#v", err)
}

defer client.Stop()

result, err := client.Search(context.Background(), "tweet", "search query", []string{"message"}, nil)
if err != nil {
    fmt.Printf("Error: %#v", err)
}

itr := result.NewHitSourceIterator()
for itr.HasNext() {
    var v tweet
    err := itr.Next(&v)
    if err != nll {
        fmt.Printf("Error: %#v", err)
    }
    fmt.Printf("tweet: %#v", v)
}
```
