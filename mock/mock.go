package mock

import (
	"github.com/ljun20160606/gox/fs"
	"io"
	"log"
	"math/rand"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func genUrls(num int) []string {
	// urls
	us := make([]string, num)
	for i := num - 1; i >= 0; i-- {
		us[i] = "http://" + strconv.Itoa(i) + ".com/"
	}
	return us
}

const (
	// Kilobyte equal to 1024b
	Kilobyte = 1024
	// MegaByte equal to 1024kb
	Megabyte = 1024 * Kilobyte
)

type TopRecord struct {
	Step   int           `json:"step"`
	Top    int           `json:"top"`
	Size   int           `json:"size"`
	Sorted []*UrlCounter `json:"sorted"`
}

// A UrlCounter will store UrlCounter{10, http://1.com/} if has 10 http://1.com/
type UrlCounter struct {
	// exist number of url exist
	Num int `json:"num"`
	// url field
	Url string `json:"url"`
}

type UrlCounterSorter []*UrlCounter

func (u UrlCounterSorter) Len() int           { return len(u) }
func (u UrlCounterSorter) Swap(i, j int)      { u[i], u[j] = u[j], u[i] }
func (u UrlCounterSorter) Less(i, j int) bool { return u[i].Num > u[j].Num }
func (u *UrlCounterSorter) Push(x interface{}) {
	*u = append(*u, x.(*UrlCounter))
}
func (u *UrlCounterSorter) Pop() interface{} {
	i := u.Len()
	p := (*u)[i-1]
	*u = (*u)[:i-1]
	return p
}

// genAndRecord is a method that generates TopRecord
// For getting a batch of discrete urls, generating 100 * topNum urls,
// then random select a url append to output
// topNum is number of expected rank
// fileSize is number of expected using max size, value is in MB
func genAndRecord(topNum int, fileSize int, output io.Writer) *TopRecord {
	topLimit := topNum * 100
	urls := genUrls(topLimit)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	urlCounters := make([]*UrlCounter, topLimit)
	for i := range urlCounters {
		// Init Url
		urlCounters[i] = &UrlCounter{Url: urls[i]}
	}
	capLimit := fileSize * Megabyte
	// Number of Bytes
	var size int
	for {
		index := r.Intn(topLimit)
		counterPtr := urlCounters[index]
		urlBytes := append([]byte(counterPtr.Url), '\n')
		size += len(urlBytes)
		if size > capLimit {
			break
		}
		counterPtr.Num += 1
		_, _ = output.Write(urlBytes)
	}
	sort.Sort(UrlCounterSorter(urlCounters))
	return &TopRecord{
		Top:    topNum,
		Size:   fileSize,
		Sorted: urlCounters[:topNum],
	}
}

type Checkpoint struct {
	CurrentStep int          `json:"currentStep"`
	TopRecords  []*TopRecord `json:"topRecords"`
}

func (c *Checkpoint) MockDataPath() string {
	return buildMockFilePath(c.CurrentStep)
}

func (c *Checkpoint) MockKey() string {
	return buildMockKey(c.CurrentStep)
}

func buildMockFilePath(step int) string {
	return buildMockKey(step) + ".txt"
}

func buildMockKey(step int) string {
	return "mock-" + strconv.Itoa(step)
}

func ReadCheckPoint() (*Checkpoint, error) {
	checkpoint := new(Checkpoint)
	err := fs.ReadJSON(CheckpointPath, checkpoint)
	if err != nil {
		log.Println(CheckpointPath + " is not found")
	}
	return checkpoint, err
}

func writeCheckPoint(checkpoint *Checkpoint) {
	_ = fs.WriteJSON(CheckpointPath, checkpoint)
}

const CheckpointPath = "checkpoint.json"

func GenMockData(topNum int, size int) *Checkpoint {
	checkpoint, _ := ReadCheckPoint()
	nextStep := checkpoint.CurrentStep + 1
	_ = fs.WriteFile(buildMockFilePath(nextStep), func(writer io.Writer) error {
		record := genAndRecord(topNum, size, writer)
		record.Step = nextStep
		checkpoint.TopRecords = append(checkpoint.TopRecords, record)
		return nil
	})
	checkpoint.CurrentStep = nextStep
	writeCheckPoint(checkpoint)
	return checkpoint
}

func IndexEqual(path1, path2 string) bool {
	_, file1 := filepath.Split(path1)
	_, file2 := filepath.Split(path2)
	prefix1 := strings.SplitN(file1, "-", 2)[0]
	prefix2 := strings.SplitN(file2, "-", 2)[0]
	return prefix1 == prefix2
}
