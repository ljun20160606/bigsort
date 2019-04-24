package bigsort

import (
	"bufio"
	"github.com/ljun20160606/bigsort/mock"
	"io"
	"sort"
	"strings"
)

// ReadTopK specifies read data by line from reader and record rate,
// return first k
func ReadTopK(k int, rs ...io.ReadCloser) ([]*mock.UrlCounter, error) {
	urlMap := make(map[string]int)
	for i := range rs {
		readCloser := rs[i]
		reader := bufio.NewReader(readCloser)
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
		// release resource
		_ = readCloser.Close()
		if err != nil {
			return nil, err
		}
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
