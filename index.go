package esmini

import "github.com/olivere/elastic"

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
