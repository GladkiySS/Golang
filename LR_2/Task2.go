package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"task2/functions"
)

func main() {
	var (
		inputFileName  = flag.String("i", "", "Use a file with the name file-name as an input.")
		outputFileName = flag.String("o", "", "Use a file with the name file-name as an output.")
		ignoreHeader   = flag.Bool("h", false, "The first line is a header that must be ignored during sorting but included in the output.")
		sortingField   = flag.Int("f", 0, "Sort input lines by value number N.")
		reverseSort    = flag.Bool("r", false, "Sort input lines in reverse order.")
		dirName        = flag.String("d", "", "dir-name that specifies a directory where it must read input files from.")
	)
	flag.Parse()
	fmt.Println("===Started===")
	done := make(chan struct{})
	ListenSignal(done)
	var (
		inputFileIsPresent      = inputFileName != nil && *inputFileName != ""
		outputFileNameIsPresent = outputFileName != nil && *outputFileName != ""
		dirNameIsPresent        = dirName != nil && *dirName != ""
	)

	if inputFileIsPresent && dirNameIsPresent {
		log.Fatal("The application must print an error message, if two options -d and -i are set at the same time.")
	}

	if !dirNameIsPresent {
		var records [][]string
		if !inputFileIsPresent {
			records = functions.WriteRecords()
		} else {
			records = functions.ReadCsvFile(*inputFileName)
		}
		functions.SortCsvData(records, *ignoreHeader, *reverseSort, *sortingField)
		if !outputFileNameIsPresent {
			fmt.Println(records)
		} else {
			functions.WriteCsvFile(*outputFileName, records)
		}
	} else {
		// new logic
		fnChan := ReadDir(*dirName, done)
		contChan := FileReadingStage(fnChan, 1, done)
		result := SortContent(contChan, *ignoreHeader, *reverseSort, *sortingField, done)
		records := make([][]string, 0, 1000)
		for i := range result {
			records = append(records, i)
		}
		if !outputFileNameIsPresent {
			fmt.Println(records)
		} else {
			functions.WriteCsvFile(*outputFileName, records)
		}
	}
	fmt.Println("===Finished===")
}

func ReadDir(dir string, done chan struct{}) (fnames chan string) {
	fnames = make(chan string)
	go func() {
		defer close(fnames)
		fileList := ScanDir(dir)
		for _, f := range fileList {
			select {
			case fnames <- f:
				{
					continue
				}
			case <-done:
				{
					break
				}
			}
		}
	}()
	return fnames
}

func FileReadingStage(fnames chan string, n int, done chan struct{}) (allLines chan [][]string) {
	lines := make([]chan [][]string, n)
	allLines = make(chan [][]string)
	for i := 0; i < n; i++ {
		lines[i] = make(chan [][]string)
		ReadFiles(fnames, lines[i], done)
	}
	go func() {
		defer close(allLines)
		wg := &sync.WaitGroup{}
		for i := range lines {
			wg.Add(1)
			go func(ch chan [][]string) {
				defer wg.Done()
				for line := range ch {
					select {
					case allLines <- line:
						{
							continue
						}
					case <-done:
						{
							break
						}
					}
				}
			}(lines[i])
		}
		wg.Wait()
	}()
	return allLines
}

func ReadFiles(fnames chan string, lines chan [][]string, done chan struct{}) {
	go func() {
		defer close(lines)
		for fname := range fnames {
			select {
			case lines <- functions.ReadCsvFile(fname):
				{
					continue
				}
			case <-done:
				{
					break
				}
			}
		}
	}()
}

func SortContent(cont chan [][]string, ignoreHeader, reverse bool, field int, done chan struct{}) (result chan []string) {
	result = make(chan []string)
	go func() {
		defer close(result)
		var buffer = make([][]string, 0, 1000)
		for line := range cont {
			buffer = append(buffer, line...)
		}
		functions.SortCsvData(buffer, ignoreHeader, reverse, field)
		for _, line := range buffer {
			select {
			case result <- line:
				{
					continue
				}
			case <-done:
				{
					break
				}
			}
		}
	}()
	return result
}

func ScanDir(path string) (files []string) {
	filepath.Walk(path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Fatalf(err.Error())
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})

	return files
}

func ListenSignal(done chan struct{}) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		sig := <-sigs
		fmt.Println()
		fmt.Println(sig)
		close(done)
	}()
}
