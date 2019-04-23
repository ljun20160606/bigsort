package bigsort

import (
	"bufio"
	"github.com/dgryski/go-farm"
	"github.com/ljun20160606/bigsort/mock"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

type NameSolver interface {
	solve(string) string
}

type prefixSolver struct {
	Prefix string
}

func (p *prefixSolver) solve(n string) string {
	return p.Prefix + n
}

func PrefixSolver(prefix string) NameSolver {
	return &prefixSolver{
		Prefix: prefix,
	}
}

type SplitConfig struct {
	// LimitMemory specifies the max amount of single file
	LimitMemory int

	// BucketSum specifies expected number of split files.
	BucketSum uint32

	// NameFunc used to modify file path
	NameSolver NameSolver
}

// Max split files is 1gb / 100kb = 10485.76
// r is reader of bigfile
func hashSplit(r io.Reader, config *SplitConfig) error {
	bucketMap := make(map[uint32]*Bucket)
	capLimit := config.LimitMemory * mock.Megabyte
	err := readFile(r, func(line []byte) error {
		i := farm.Hash32(line) % config.BucketSum
		bucket, has := bucketMap[i]
		if !has {
			bucket, err := OpenBucket(int(i), 0, config.NameSolver)
			if err != nil {
				return err
			}
			bucketMap[i] = bucket
			return nil
		}
		size := bucket.Size + len(line) + 1
		// Bucket too large, close old bucket, new a child bucket
		if size > capLimit {
			_ = bucket.File.Close()
			bucket, _ = bucket.OpenSubBucket(config.NameSolver)
			bucketMap[i] = bucket
			size = len(line) + 1
		}
		// write split data
		_, _ = bucket.File.Write(append(line, '\n'))
		// record size
		bucket.Size = size

		return nil
	})
	for i := range bucketMap {
		// close all file
		_ = bucketMap[i].File.Close()
	}
	return err
}

func OpenBucket(index, subIndex int, solver NameSolver) (*Bucket, error) {
	bucket := &Bucket{
		Index:    index,
		SubIndex: subIndex,
	}
	err := bucket.OpenFile(solver)
	return bucket, err
}

type Bucket struct {
	Index    int
	SubIndex int
	Size     int
	File     io.WriteCloser
}

func (b *Bucket) OpenFile(solver NameSolver) error {
	name := strconv.Itoa(b.Index) + "-" + strconv.Itoa(b.SubIndex) + ".txt"
	if solver != nil {
		name = solver.solve(name)
	}
	dir, _ := filepath.Split(name)
	err := os.MkdirAll(dir, 0751)
	if err != nil {
		return err
	}
	file, err := os.Create(name)
	if err != nil {
		return err
	}
	b.File = file
	return nil
}

func (b *Bucket) OpenSubBucket(solver NameSolver) (*Bucket, error) {
	bucket := &Bucket{
		Index:    b.Index,
		SubIndex: b.SubIndex + 1,
	}
	err := bucket.OpenFile(solver)
	return bucket, err
}

func readFile(r io.Reader, handleLine func(line []byte) error) error {
	buf := bufio.NewReader(r)
	for {
		line, _, err := buf.ReadLine()
		if err != nil {
			if err != io.EOF {
				return err
			}
			break
		}
		err = handleLine(line)
		if err != nil {
			return err
		}
	}
	return nil
}
