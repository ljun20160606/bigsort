package main

import (
	"github.com/ljun20160606/bigsort"
	"github.com/ljun20160606/bigsort/mock"
	"github.com/ljun20160606/gox/fs"
	"github.com/pkg/errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
)

func splitMock(limitMemory int, bucketSum uint32, checkpoint *mock.Checkpoint) (string, error) {
	path := checkpoint.MockDataPath()
	log.Printf("Split mock %v", path)
	splitDirectory := "split-" + checkpoint.MockKey()
	log.Println("Generate split mock data: ", splitDirectory)
	prefix := splitDirectory + "/"
	if !fs.Exists(splitDirectory) {
		err := fs.ReadFile(path, func(reader io.Reader) error {
			err := bigsort.HashSplit(
				reader,
				&bigsort.SplitConfig{LimitMemory: limitMemory, BucketSum: bucketSum, NameSolver: bigsort.PrefixSolver(prefix)},
			)
			if err != nil {
				return err
			}
			return nil
		})
		if err != nil {
			return "", errors.Wrap(err, "read mock fail.")
		}
	}
	return prefix, nil
}

func computeSplitData(prefix string, topNum int) ([]*mock.UrlCounter, error) {
	log.Println("Compute split data topK")
	globalCounters := make(map[string]*mock.UrlCounter)
	var stack []string
	err := filepath.Walk(prefix, func(path string, info os.FileInfo, err error) error {
		if prefix == path {
			return nil
		}
		if len(stack) == 0 ||
			mock.IndexEqual(stack[len(stack)-1], path) {
			stack = append(stack, path)
			return nil
		}

		err = readTopKFromFile(topNum, globalCounters, stack)
		// clear stack
		stack = stack[:0]
		stack = append(stack, path)
		return err
	})

	if err != nil {
		return nil, err
	}

	if len(stack) != 0 {
		err = readTopKFromFile(topNum, globalCounters, stack)
		if err != nil {
			return nil, err
		}
	}

	result := make([]*mock.UrlCounter, 0, len(globalCounters))
	for key := range globalCounters {
		counter := globalCounters[key]
		result = append(result, counter)
	}
	return result, nil
}

func readTopKFromFile(topNum int, globalCounters map[string]*mock.UrlCounter, stack []string) error {
	rs := make([]io.ReadCloser, 0, len(stack))
	for i := range stack {
		file, err := os.OpenFile(stack[i], os.O_RDONLY, 0)
		if err != nil {
			return err
		}
		rs = append(rs, file)
	}

	counters, err := bigsort.ReadTopK(topNum, rs...)
	if err != nil {
		return errors.Wrap(err, "readTopK fail "+strings.Join(stack, ", "))
	}
	for i := range counters {
		counter := counters[i]
		if urlCounter, has := globalCounters[counter.Url]; has {
			urlCounter.Num += counter.Num
		} else {
			globalCounters[counter.Url] = counter
		}
	}

	return nil
}
