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
	index   int
}

type SearchOrder int

const (
	Asc SearchOrder = iota
	Desc
)

const (
	DefaultSize      = 100
	DefaultFrom      = 0
	DefaultFuzziness = "AUTO"
)

type searchOption struct {
	size               int
	from               int
	sortField          string
	order              SearchOrder
	matchType          string
	fuzziness          string
	minimumShouldMatch string
	boolClause         string
	boolQueries        map[string]interface{}
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

func BoolQueries(queries map[string]interface{}) SearchOption {
	return func(s *searchOption) {
		s.boolQueries = queries
	}
}

func Fuzziness(fuzziness string) SearchOption {
	return func(s *searchOption) {
		s.fuzziness = fuzziness
	}
}

func MinimumShouldMatch(minimumShouldMatch string) SearchOption {
	return func(s *searchOption) {
		s.minimumShouldMatch = minimumShouldMatch
	}
}

// BoolClause can be "must", "should", "must_not", "filter"
func BoolClause(boolClause string) SearchOption {
	return func(s *searchOption) {
		s.boolClause = boolClause
	}
}

func NewSearchClient(client *IndexClient) *SearchClient {
	return &SearchClient{
		iClient: client,
	}
}

func (s *SearchClient) Search(ctx context.Context, index string, searchText interface{}, targetFields []string, opts ...SearchOption) (SearchResponse, error) {
	sOpt := &searchOption{
		size:      DefaultSize,
		from:      DefaultFrom,
		fuzziness: DefaultFuzziness,
	}
	for _, opt := range opts {
		opt(sOpt)
	}

	query := elastic.NewBoolQuery()
	if searchText != "" {
		multiMatchQuery := elastic.NewMultiMatchQuery(searchText, targetFields...).Type(sOpt.matchType)
		if sOpt.matchType == "cross_fields" {
			multiMatchQuery.
				Fuzziness(sOpt.fuzziness)
		} else {
			multiMatchQuery.
				Fuzziness(sOpt.fuzziness).
				MinimumShouldMatch(sOpt.minimumShouldMatch)
		}
		query.Must(multiMatchQuery)
	}

	if len(sOpt.boolQueries) > 0 {
		for key, value := range sOpt.boolQueries {
			if values, ok := value.([]string); ok {
				for _, v := range values {
					termQuery := elastic.NewTermQuery(key, v)
					switch sOpt.boolClause {
					case "must":
						query.Must(termQuery)
					case "should":
						query.Should(termQuery)
					case "must_not":
						query.MustNot(termQuery)
					default:
						query.Filter(termQuery)
					}
				}
			} else {
				query.Filter(elastic.NewTermQuery(key, value))
			}
		}
	}

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

	result.Hits = int64(len(res.Hits.Hits))

	for _, hit := range res.Hits.Hits {
		result.Sources = append(result.Sources, hit.Source)
	}

	return result, nil
}

func (r *SearchResponse) NewHitSourceIterator() *HitSourceIterator {
	return &HitSourceIterator{
		array: r.Sources,
		index: 0,
	}
}

type HitSourceIterator struct {
	array []json.RawMessage
	index int
}

func (i *HitSourceIterator) Index() int {
	return i.index
}

func (i *HitSourceIterator) HasNext() bool {
	return i.index != len(i.array)
}

func (i *HitSourceIterator) Next(v interface{}) error {
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
