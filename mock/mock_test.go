package mock

import (
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"testing"
)

func TestGenUrls(t *testing.T) {
	ast := assert.New(t)
	urls := genUrls(2)
	ast.Equal([]string{"http://0.com/", "http://1.com/"}, urls)
}

func TestGenAndRecord(t *testing.T) {
	ast := assert.New(t)
	rankNum := 3
	records := genAndRecord(rankNum, 1, ioutil.Discard)
	counters := records.Sorted
	ast.True(counters[0].Num >= counters[1].Num)
	ast.True(counters[1].Num >= counters[2].Num)
	ast.True(len(counters) == rankNum)
}

func TestGenMockData(t *testing.T) {
	GenMockData(10, 1)
}
