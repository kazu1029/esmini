package esmini

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/olivere/elastic/v7"
)

type SearchClient struct {
	iClient *IndexClient
}

type SearchResponse struct {
	Hits    int64
	Results []json.RawMessage
}

type SearchOrder int

const (
	Asc SearchOrder = iota
	Desc
)

const DefaultSize = 100
const DefaultStartLoc = 0

type searchOption struct {
	size      int
	startLoc  int
	sortField string
	order     SearchOrder
	matchType string
}

type SearchOption func(*searchOption)

func Limit(size int) SearchOption {
	return func(s *searchOption) {
		s.size = size
	}
}

func Page(startLoc int) SearchOption {
	return func(s *searchOption) {
		s.startLoc = startLoc
	}
}

func SortField(sortField string) SearchOption {
	return func(s *searchOption) {
		s.sortField = sortField
	}
}

func Order(order SearchOrder) SearchOption {
	return func(s *searchOption) {
		s.order = order
	}
}

func MatchTyp(matchType string) SearchOption {
	return func(s *searchOption) {
		s.matchType = matchType
	}
}

func NewSearchClient(client *IndexClient) *SearchClient {
	return &SearchClient{
		iClient: client,
	}
}

func (s *SearchClient) Search(ctx context.Context, index string, searchText interface{}, targetFields []string, opts ...SearchOption) (SearchResponse, error) {
	sOpt := &searchOption{
		size:     DefaultSize,
		startLoc: DefaultStartLoc,
	}
	for _, opt := range opts {
		opt(sOpt)
	}

	query := elastic.NewMultiMatchQuery(searchText, targetFields...).
		Type(sOpt.matchType).
		Fuzziness("AUTO").
		MinimumShouldMatch("2")

	var sortQuery *elastic.FieldSort
	var res *elastic.SearchResult
	var err error

	if len(sOpt.sortField) > 0 {
		if sOpt.order == Asc {
			sortQuery = elastic.NewFieldSort(sOpt.sortField).Asc()
		} else {
			sortQuery = elastic.NewFieldSort(sOpt.sortField).Desc()
		}
		res, err = s.iClient.raw.Search().
			Index(index).
			Query(query).
			SortBy(sortQuery).
			From(sOpt.startLoc).Size(sOpt.size).
			Do(ctx)
	} else {
		res, err = s.iClient.raw.Search().
			Index(index).
			Query(query).
			From(sOpt.startLoc).Size(sOpt.size).
			Do(ctx)
	}

	var result SearchResponse

	if err != nil {
		return result, err
	}

	result.Hits = res.Hits.TotalHits.Value

	for _, hit := range res.Hits.Hits {
		result.Results = append(result.Results, hit.Source)
	}

	return result, nil
}

type Iterator interface {
	Index() int
	HasNext() bool
	Next(v interface{}) error
}

type SearchResultIterator struct {
	array []json.RawMessage
	index int
}

func (i *SearchResultIterator) Index() int {
	return i.index
}

func (i *SearchResultIterator) HasNext() bool {
	return i.index != len(i.array)
}

func (i *SearchResultIterator) Next(v interface{}) error {
	if i.HasNext() {
		bytes := []byte(i.array[i.index])
		_ = json.Unmarshal(bytes, v)
		i.index++
		return nil
	}
	return errors.New("No next value")
}
