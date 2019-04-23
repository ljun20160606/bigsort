package bigsort

import (
	"fmt"
	"github.com/ljun20160606/bigsort/mock"
	"github.com/ljun20160606/gox/fs"
	"io"
	"testing"
)

func TestName(t *testing.T) {
	mock.GenMockData(10, 10)
}

func TestHashSplit(t *testing.T) {
	checkPoint := mock.ReadCheckPoint()
	_ = fs.ReadFile(checkPoint.MockDataPath(), func(reader io.Reader) error {
		prefix := "split-" + checkPoint.MockKey() + "/"
		err := hashSplit(reader, &SplitConfig{1, 10, PrefixSolver(prefix)})
		if err != nil {
			fmt.Println(err)
		}
		return nil
	})
}
