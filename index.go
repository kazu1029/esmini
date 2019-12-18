package esmini

import (
	"container/list"
	"context"

	"github.com/olivere/elastic/v7"
)

type IndexClient struct {
	client *elastic.Client
}

func New(options ...elastic.ClientOptionFunc) (*IndexClient, error) {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return nil, err
	}

	return &IndexClient{client: client}, nil
}

func (i *IndexClient) CreateIndex(ctx context.Context, index string) (*elastic.IndicesCreateResult, error) {
	return i.client.CreateIndex(index).Do(ctx)
}

func (i *IndexClient) CreateIndexWithMapping(ctx context.Context, index, mapping string) (*elastic.IndicesCreateResult, error) {
	return i.client.CreateIndex(index).
		BodyJson(mapping).
		Do(ctx)
}

func (i *IndexClient) CreateTemplate(ctx context.Context, tempName, template string) (*elastic.IndicesPutTemplateResponse, error) {
	return i.client.IndexPutTemplate(tempName).
		BodyString(template).
		Do(ctx)
}

type bulkOption struct {
	pipeline string
}

type BulkOption func(*bulkOption)

func Pipeline(pipeline string) BulkOption {
	return func(b *bulkOption) {
		b.pipeline = pipeline
	}
}

func (i *IndexClient) BulkInsert(ctx context.Context, index string, docs *list.List, opts ...BulkOption) (*elastic.BulkResponse, error) {
	bulkOpt := &bulkOption{}
	for _, opt := range opts {
		opt(bulkOpt)
	}

	bulk := i.client.Bulk().
		Index(index).
		Pipeline(bulkOpt.pipeline)

	for d := docs.Front(); d != nil; d = d.Next() {
		bulk = bulk.Add(elastic.NewBulkIndexRequest().Index(index).Doc(d.Value))
	}

	res, err := bulk.Do(ctx)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (i *IndexClient) Ping(ctx context.Context, host string) (*elastic.PingResult, int, error) {
	return i.client.Ping(host).Do(ctx)
}

func (i *IndexClient) Stop() {
	i.client.Stop()
}
