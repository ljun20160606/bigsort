package bigsort

import (
	"bufio"
	"github.com/ljun20160606/bigsort/mock"
	"io"
	"sort"
	"strings"
)

// readTopK specifies read data by line from reader and record rate,
// return first k
func readTopK(r io.Reader, k int) ([]*mock.UrlCounter, error) {
	reader := bufio.NewReader(r)
	urlMap := make(map[string]int)
	err := readFile(reader, func(line []byte) error {
		key := string(line)
		// ignore space
		if strings.TrimSpace(key) == "" {
			return nil
		}
		if _, has := urlMap[key]; has {
			urlMap[key]++
		} else {
			urlMap[key] = 1
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	container := make([]*mock.UrlCounter, 0, len(urlMap))
	for key := range urlMap {
		container = append(container, &mock.UrlCounter{
			Url: key,
			Num: urlMap[key],
		})
	}

	sort.Sort(mock.UrlCounterSorter(container))
	if k > len(container) {
		return container, nil
	}
	return container[:k], nil
}

