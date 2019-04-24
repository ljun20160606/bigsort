package main

import (
	"encoding/json"
	"fmt"
	"github.com/ljun20160606/bigsort/mock"
	"github.com/spf13/cobra"
	"log"
	"sort"
	"strconv"
)

var (
	rootCmd = &cobra.Command{}
)

func mockFunc(cmd *cobra.Command, args []string) {
	topNumStr := cmd.Flags().Lookup("topNum").Value.String()
	sizeStr := cmd.Flags().Lookup("size").Value.String()
	topNum, _ := strconv.Atoi(topNumStr)
	size, _ := strconv.Atoi(sizeStr)
	log.Printf("Generate mock topNum: %v cap: %v\n", topNum, size)
	checkpoint := mock.GenMockData(topNum, size)
	log.Printf("Generate mock name: %v checkpoint: %v\n", checkpoint.MockDataPath(), mock.CheckpointPath)
}

func computeFunc(cmd *cobra.Command, args []string) {
	topNumStr := cmd.Flags().Lookup("topNum").Value.String()
	topNum, _ := strconv.Atoi(topNumStr)
	limitMemoryStr := cmd.Flags().Lookup("limitMemory").Value.String()
	limitMemory, _ := strconv.Atoi(limitMemoryStr)
	bucketSumStr := cmd.Flags().Lookup("bucketSum").Value.String()
	bucketSum, _ := strconv.Atoi(bucketSumStr)

	log.Println("Read checkpoint")
	checkpoint, err := mock.ReadCheckPoint()
	if err != nil {
		return
	}

	prefix, err := splitMock(limitMemory, uint32(bucketSum), checkpoint)
	if err != nil {
		log.Println(err)
		return
	}

	result, err := computeSplitData(prefix, topNum)
	if err != nil {
		log.Println(err)
		return
	}

	sort.Sort(mock.UrlCounterSorter(result))

	bytes, _ := json.Marshal(result[:topNum])
	log.Println("Get result")
	log.Println(string(bytes))
	log.Println("Checkpoint")
	expectedBytes, _ := json.Marshal(checkpoint.TopRecords[len(checkpoint.TopRecords)-1].Sorted)
	log.Println(string(expectedBytes))
}

func init() {
	mockCmd := &cobra.Command{
		Use:   "mock",
		Short: "Generate mock data",
		Run:   mockFunc,
	}
	mockCmd.Flags().IntP("topNum", "t", 10, "Expected first number of url")
	mockCmd.Flags().IntP("size", "s", 10, "big file size /M")
	rootCmd.AddCommand(mockCmd)

	computeCmd := &cobra.Command{
		Use:   "compute",
		Short: "Compute top k",
		Run:   computeFunc,
	}
	computeCmd.Flags().IntP("topNum", "t", 10, "Expected first number of url")
	computeCmd.Flags().IntP("limitMemory", "l", 1, "Limited memory")
	computeCmd.Flags().IntP("bucketSum", "b", 10, "Split bucket")
	rootCmd.AddCommand(computeCmd)
}

func main() {
	//go log.Println(http.ListenAndServe("localhost:6060", nil))
	err := rootCmd.Execute()
	if err != nil {
		fmt.Println(err)
	}
	//c := make(chan os.Signal)
	//signal.Notify(c, os.Interrupt, os.Kill)
	//<-c
}
