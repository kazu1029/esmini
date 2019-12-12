package esmini

import (
	"context"

	"github.com/olivere/elastic/v7"
)

type IndexClient struct {
	*elastic.Client
}

func New(options ...elastic.ClientOptionFunc) (*IndexClient, error) {
	client, err := elastic.NewClient(options...)
	if err != nil {
		return &IndexClient{}, err
	}

	return &IndexClient{client}, nil
}

func (i *IndexClient) Create(ctx context.Context, index string) (*elastic.IndicesCreateResult, error) {
	createIndex, err := i.CreateIndex(index).Do(ctx)
	if err != nil {
		return nil, err
	}
	return createIndex, nil
}

func (i *IndexClient) CreateIndexWithMapping(ctx context.Context, index, mapping string) (*elastic.IndicesCreateResult, error) {
	res, err := i.CreateIndex(index).
		BodyJson(mapping).
		Do(ctx)

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (i *IndexClient) CreateTemplate(ctx context.Context, tempName, template string) error {
	if _, err := i.IndexPutTemplate(tempName).BodyString(template).Do(ctx); err != nil {
		return err
	}
	return nil
}

type BulkOption struct {
	pipeline string
}

type option func(*BulkOption)

func Pipeline(pipeline string) option {
	return func(b *BulkOption) {
		b.pipeline = pipeline
	}
}

func (i *IndexClient) BulkInsert(ctx context.Context, index string, docs []interface{}, opts ...option) error {
	bulkOption := &BulkOption{}
	for _, opt := range opts {
		opt(bulkOption)
	}

	bulk := i.Bulk().
		Index(index).
		Pipeline(bulkOption.pipeline)

	for _, d := range docs {
		bulk = bulk.Add(elastic.NewBulkIndexRequest().Index(index).Doc(d))
	}

	if _, err := bulk.Do(ctx); err != nil {
		return err
	}

	return nil
}
