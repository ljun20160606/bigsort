package bigsort

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestReadTopK(t *testing.T) {
	ast := assert.New(t)

	reader := bytes.NewReader([]byte(`http://339.com/
http://339.com/
http://339.com/
http://429.com/
http://429.com/
http://475.com/
http://98.com/
http://806.com/`))
	counters, _ := ReadTopK(10, reader)

	ast.Equal(5, len(counters))
	ast.Equal("http://339.com/", counters[0].Url)
	ast.Equal("http://429.com/", counters[1].Url)
}
