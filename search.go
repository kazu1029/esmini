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
	Sources []json.RawMessage
	searchResultIterator
}

type SearchOrder int

const (
	Asc SearchOrder = iota
	Desc
)

const DefaultSize = 100
const DefaultFrom = 0

type searchOption struct {
	size      int
	from      int
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

func From(from int) SearchOption {
	return func(s *searchOption) {
		s.from = from
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

// MatchType can be "best_fields", "boolean", "most_fields", "cross_fields",
// "phrase", "phrase_prefix" or "bool_prefix"
func MatchType(matchType string) SearchOption {
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
		size: DefaultSize,
		from: DefaultFrom,
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
	var result SearchResponse

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
			From(sOpt.from).Size(sOpt.size).
			Do(ctx)

		if err != nil {
			return result, err
		}
	} else {
		res, err = s.iClient.raw.Search().
			Index(index).
			Query(query).
			From(sOpt.from).Size(sOpt.size).
			Do(ctx)

		if err != nil {
			return result, err
		}
	}

	result.Hits = res.Hits.TotalHits.Value

	for _, hit := range res.Hits.Hits {
		result.Sources = append(result.Sources, hit.Source)
	}
	result.array = result.Sources

	return result, nil
}

type Iterator interface {
	Index() int
	HasNext() bool
	Next(v interface{}) error
}

type searchResultIterator struct {
	array []json.RawMessage
	index int
}

func (i *searchResultIterator) Index() int {
	return i.index
}

func (i *searchResultIterator) HasNext() bool {
	return i.index != len(i.array)
}

func (i *searchResultIterator) Next(v interface{}) error {
	if i.HasNext() {
		bytes := []byte(i.array[i.index])
		err := json.Unmarshal(bytes, v)
		if err != nil {
			return err
		}
		i.index++
		return nil
	}
	return errors.New("No next value")
}
