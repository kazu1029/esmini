package esmini

import (
	"container/list"
	"context"
	"reflect"
	"strconv"

	"github.com/olivere/elastic/v7"
)

type IndexClient struct {
	raw *elastic.Client
}

func New(options ...elastic.ClientOptionFunc) (*IndexClient, error) {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return &IndexClient{raw: client}, nil
}

func (i *IndexClient) CreateIndex(ctx context.Context, index string) (*elastic.IndicesCreateResult, error) {
	return i.raw.CreateIndex(index).Do(ctx)
}

func (i *IndexClient) CreateIndexWithMapping(ctx context.Context, index, mapping string) (*elastic.IndicesCreateResult, error) {
	return i.raw.CreateIndex(index).
		BodyJson(mapping).
		Do(ctx)
}

func (i *IndexClient) CreateTemplate(ctx context.Context, tempName, template string) (*elastic.IndicesPutTemplateResponse, error) {
	return i.raw.IndexPutTemplate(tempName).
		BodyString(template).
		Do(ctx)
}

type bulkOption struct {
	pipeline string
	docID    string
}

type BulkOption func(*bulkOption)

func Pipeline(pipeline string) BulkOption {
	return func(b *bulkOption) {
		b.pipeline = pipeline
	}
}

func DocID(docID string) BulkOption {
	return func(b *bulkOption) {
		b.docID = docID
	}
}

func (i *IndexClient) BulkInsert(ctx context.Context, index string, docs *list.List, opts ...BulkOption) (*elastic.BulkResponse, error) {
	bulkOpt := &bulkOption{}
	for _, opt := range opts {
		opt(bulkOpt)
	}

	bulk := i.raw.Bulk().
		Index(index).
		Pipeline(bulkOpt.pipeline)

	for d := docs.Front(); d != nil; d = d.Next() {
		if len(bulkOpt.docID) > 0 {
			var docID string
			v := reflect.Indirect(reflect.ValueOf(d.Value))
			t := v.Type()
			for j := 0; j < t.NumField(); j++ {
				if t.Field(j).Name == bulkOpt.docID {
					if value, ok := v.Field(j).Interface().(int); ok {
						docID = strconv.Itoa(value)
					} else if value, ok := v.Field(j).Interface().(uint64); ok {
						docID = strconv.FormatUint(value, 10)
					} else {
						docID = v.Field(j).String()
					}
					break
				}
			}
			bulk = bulk.Add(elastic.NewBulkIndexRequest().Index(index).Id(docID).Doc(d.Value))
		} else {
			bulk = bulk.Add(elastic.NewBulkIndexRequest().Index(index).Doc(d.Value))
		}
	}

	res, err := bulk.Do(ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (i *IndexClient) Update(ctx context.Context, index string, id string, doc map[string]interface{}) (*elastic.UpdateResponse, error) {
	return i.raw.Update().
		Index(index).
		Id(id).
		Doc(doc).
		Refresh("true").
		Do(ctx)
}

func (i *IndexClient) DeleteIndex(ctx context.Context, index string) (*elastic.IndicesDeleteResponse, error) {
	return i.raw.DeleteIndex(index).Do(ctx)
}

func (i *IndexClient) Delete(ctx context.Context, index, id string) (*elastic.DeleteResponse, error) {
	return i.raw.Delete().
		Index(index).
		Id(id).
		Refresh("true").
		Do(ctx)
}

func (i *IndexClient) Ping(ctx context.Context, host string) (*elastic.PingResult, int, error) {
	return i.raw.Ping(host).Do(ctx)
}

func (i *IndexClient) Stop() {
	i.raw.Stop()
}
