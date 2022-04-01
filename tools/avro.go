package tools

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/hamba/avro/ocf"
	"log"
	"math"
	"math/rand"
	"os"
	"time"
)

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

// RandStringBytes Generate a string of random bytes
func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

const schemaStr = `{
  "type": "record",
  "name": "tbl",
  "fields": [
    {
      "name": "field01",
      "type": "string"
    },
    {
      "name": "field02",
      "type": "int"
    },
    {
      "name": "field03",
      "type": "int"
    },
    {
      "name": "field04",
      "type": "int"
    },
    {
      "name": "field05",
      "type": "int"
    },
    {
      "name": "field06",
      "type": "int"
    },
    {
      "name": "field07",
      "type": "int"
    },
    {
      "name": "field08",
      "type": "int"
    },
    {
      "name": "field09",
      "type": "int"
    },
    {
      "name": "field10",
      "type": "int"
    },
    {
      "name": "field11",
      "type": "int"
    },
    {
      "name": "field12",
      "type": "int"
    },
    {
      "name": "field13",
      "type": "int"
    },
    {
      "name": "field14",
      "type": "string"
    },
    {
      "name": "field15",
      "type": "int"
    },
    {
      "name": "field16",
      "type": "string"
    },
    {
      "name": "field17",
      "type": "boolean"
    },
    {
      "name": "field18",
      "type": "boolean"
    },
    {
      "name": "field19",
      "type": "int"
    },
    {
      "name": "field20",
      "type": "int"
    },
    {
      "name": "field21",
      "type": "int"
    },
    {
      "name": "field22",
      "type": "bytes"
    },
    {
      "name": "field23",
      "type": "int"
    },
    {
      "name": "field24",
      "type": "int"
    },
    {
      "name": "field25",
      "type": "int"
    },
    {
      "name": "field26",
      "type": "string"
    }
  ]
}`

const letters = "abcdefghijklmnopqrstuvwxyz"
const letterCnt = len(letters)

type TblRecord struct {
	Field01 string `avro:"field01"`
	Field02 int    `avro:"field02"`
	Field03 int    `avro:"field03"`
	Field04 int    `avro:"field04"`
	Field05 int    `avro:"field05"`
	Field06 int    `avro:"field06"`
	Field07 int    `avro:"field07"`
	Field08 int    `avro:"field08"`
	Field09 int    `avro:"field09"`
	Field10 int    `avro:"field10"`
	Field11 int    `avro:"field11"`
	Field12 int    `avro:"field12"`
	Field13 int    `avro:"field13"`
	Field14 string `avro:"field14"`
	Field15 int    `avro:"field15"`
	Field16 string `avro:"field16"`
	Field17 bool   `avro:"field17"`
	Field18 bool   `avro:"field18"`
	Field19 int    `avro:"field19"`
	Field20 int    `avro:"field20"`
	Field21 int    `avro:"field21"`
	Field22 []byte `avro:"field22"`
	Field23 int    `avro:"field23"`
	Field24 int    `avro:"field24"`
	Field25 int    `avro:"field25"`
	Field26 string `avro:"field26"`
}

func GenerateRecord(sorted bool, fileCount int, maxFiles int, recordCount int, maxRecords int) TblRecord {

	field01 := RandStringBytes(10)
	if sorted {
		field01 = GenerateOrderedField01(fileCount, maxFiles) + GenerateOrderedField01(recordCount, maxRecords)
	}

	return TblRecord{
		Field01: field01,
		Field02: rand.Int(),
		Field03: rand.Int(),
		Field04: rand.Int(),
		Field05: rand.Int(),
		Field06: rand.Int(),
		Field07: rand.Int(),
		Field08: rand.Int(),
		Field09: rand.Int(),
		Field10: rand.Int(),
		Field11: rand.Int(),
		Field12: rand.Int(),
		Field13: rand.Int(),
		Field14: RandStringBytes(10),
		Field15: rand.Int(),
		Field16: time.Now().Format(time.RFC3339),
		Field17: rand.Intn(2) == 1,
		Field18: rand.Intn(2) == 1,
		Field19: rand.Int(),
		Field20: rand.Int(),
		Field21: rand.Int(),
		Field22: []byte(RandStringBytes(10)),
		Field23: rand.Int(),
		Field24: rand.Int(),
		Field25: rand.Int(),
		Field26: time.Now().Format(time.RFC3339),
	}

}

func ReadAvroFile(filepath string) {
	f, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	dec, err := ocf.NewDecoder(f)
	if err != nil {
		log.Fatal(err)
	}

	for dec.HasNext() {
		var record TblRecord
		err = dec.Decode(&record)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(record)
	}

	if dec.Error() != nil {
		log.Fatal(dec.Error())
	}
}

func AvroFilePath(fileCounter int, localDirectory string) string {
	return fmt.Sprintf("%s/tbl-%05d.avro", localDirectory, fileCounter)
}

func CloudFilePath(fileCounter int) string {
	return fmt.Sprintf("tbl-%05d.avro", fileCounter)
}

func GenerateOrderedField01(cnt int, max int) string {

	chars := 1

	// Add an extra character for each power
	for int(math.Pow(float64(letterCnt), float64(chars))) < max {
		chars++
	}

	result := ""

	for remainingChars := chars; remainingChars > 0; remainingChars-- {

		lindex := (int(math.Ceil(float64(cnt)/math.Pow(float64(letterCnt), float64(remainingChars-1)))) - 1) % letterCnt
		l := string(letters[lindex])
		result = result + l
	}

	return result
}

func WriteRecords(fileCount int, maxFiles int, filesizeMB int, storageBucket string, bucketPrefix string, sorted bool, localDirectory string) {

	// TODO: we need an upper bound of records to be able to generated sorted data
	// If we created files too large then this may be exceed, however, we will stop
	// generating data and return if we hit this max
	maxRecords := 1000000000

	// Open local file
	filePath := AvroFilePath(fileCount, localDirectory)
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	defer func(f *os.File) {
		err := f.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(f)

	encLocal, err := ocf.NewEncoder(schemaStr, f)
	if err != nil {
		log.Fatal(err)
	}

	// Open gcs file
	cloudFilePath := CloudFilePath(fileCount)
	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatal(err)
	}
	bkt := client.Bucket(storageBucket)
	obj := bkt.Object(fmt.Sprintf("%s/%s", bucketPrefix, cloudFilePath))
	w := obj.NewWriter(ctx)
	defer func(w *storage.Writer) {
		err := w.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(w)

	encCloud, err := ocf.NewEncoder(schemaStr, w)
	if err != nil {
		log.Fatal(err)
	}
	recordCount := 0
	// Run until we get to the file size
	for {

		recordCount++
		record := GenerateRecord(sorted, fileCount, maxFiles, recordCount, maxRecords)

		// Write locally
		err = encLocal.Encode(record)
		if err != nil {
			log.Fatal(err)
		}

		// Write to cloud
		err = encCloud.Encode(record)
		if err != nil {
			log.Fatal(err)
		}

		if recordCount%1000 == 0 {

			if err := encLocal.Flush(); err != nil {
				log.Fatal(err)
			}

			if err := f.Sync(); err != nil {
				log.Fatal(err)
			}

			// Get length of file in bytes
			stat, err := f.Stat()
			if err != nil {
				log.Fatal(err)
			}

			bytes := stat.Size()
			kb := bytes / 1024
			mb := kb / 1024
			if int(mb) >= filesizeMB || recordCount > maxRecords {
				return
			}

		}

	}
}

// Our test input is a file set consisting of 235 avro files, 24 GB in total.
// -- 100MiB each
// -- 4x number of nodes
// For testing, 10 nodes, so maybe 40 files?
// Let's keep the files the same size
func GenerateAvroFiles(numFile int, fileSizeMb int, storageBucket string, bucketPrefix string, sorted bool, localDirectory string) {

	//var wg sync.WaitGroup
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Iterating from 1 to %v\n", numFile)
	for i := 1; i <= numFile; i++ {

		//wg.Add(1)
		//go func(cnt int,waitGroup sync.WaitGroup) {
		//fmt.Println("Writing records")
		WriteRecords(i, numFile, fileSizeMb, storageBucket, bucketPrefix, sorted, localDirectory)
		//	wg.Done()
		//}(i, wg)

	}

	//wg.Wait()

}
