package main

import (
	"bufio"
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
)

func main() {
	var (
		inputFileName  = flag.String("i", "", "Use a file with the name file-name as an input.")
		outputFileName = flag.String("o", "", "Use a file with the name file-name as an output.")
		ignoreHeader   = flag.Bool("h", false, "The first line is a header that must be ignored during sorting but included in the output.")
		sortingField   = flag.Int("f", 0, "Sort input lines by value number N.")
		reverseSort    = flag.Bool("r", false, "Sort input lines in reverse order.")
	)

	flag.Parse()

	fmt.Println("===Started===")

	// Write to file
	if inputFileName != nil && *inputFileName != "" {
		records := writeRecords()
		sortCsvData(records, *ignoreHeader, *reverseSort, *sortingField)
		writeCsvFile(*inputFileName, records)
	}

	// Read from file
	if outputFileName != nil && *outputFileName != "" {
		records := readCsvFile(*outputFileName)
		sortCsvData(records, *ignoreHeader, *reverseSort, *sortingField)
		fmt.Println(records)
	}

	fmt.Println("===Finished===")
}

func readCsvFile(filePath string) [][]string {
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer f.Close()

	content := [][]string{}

	csvReader := csv.NewReader(f)
	records, err := csvReader.Read()
	for records != nil {
		if err != nil {
			fmt.Println("", err)
			os.Exit(1)
		}

		content = append(content, records)
		records, err = csvReader.Read()
	}

	return content
}

func sortCsvData(content [][]string, ignoreHeader, reverse bool, field int) {
	if field > (len(content[0]) - 1) {
		fmt.Printf("Error: only %d column in this file.\n", len(content[0]))
		os.Exit(1)
	}
	if reverse {
		if ignoreHeader {
			sort.Slice(content[1:], func(i, j int) bool {
				return content[1:][i][field] > content[1:][j][field]
			})
		} else {
			sort.Slice(content, func(i, j int) bool {
				return content[i][field] > content[j][field]
			})
		}
	} else {
		if ignoreHeader {
			sort.Slice(content[1:], func(i, j int) bool {
				return content[1:][i][field] < content[1:][j][field]
			})
		} else {
			sort.Slice(content, func(i, j int) bool {
				return content[i][field] < content[j][field]
			})
		}
	}
}

func writeCsvFile(name string, data [][]string) {
	file, err := os.Create(name)
	if err != nil {
		fmt.Println("Unable to create file:", err)
		os.Exit(1)
	}
	defer file.Close()

	csvWriter := csv.NewWriter(file)
	err = csvWriter.WriteAll(data)

	if err != nil {
		fmt.Println("Unable to create file:", err)
		os.Exit(1)
	}

	fmt.Println("File created.")
}

func writeRecords() [][]string {
	s := bufio.NewScanner(os.Stdin)

	records := [][]string{}

	n := 0

	for s.Scan() {
		line := s.Text()
		if line == "" {
			break
		}
		row := strings.Split(line, ",")

		if n == 0 {
			n = len(row)
		}

		if n != len(row) {
			fmt.Printf("Error: row has %d column, but must have %d\n", len(row), n)
			os.Exit(1)
		}

		records = append(records, row)
	}

	return records
}
