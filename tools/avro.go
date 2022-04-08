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
	"sync"
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

func GenerateFirstPrimaryKeyColumn(sorted bool, partitioned bool, fileCount int, maxFiles int, recordCount int, maxRecords int, length int) string {

	var field01 string

	if sorted && partitioned { // Give field ordered prefix for each file and ordered string
		field01 = GenerateOrderedString(fileCount, maxFiles) + GenerateOrderedString(recordCount, maxRecords)
	} else if sorted { // Give field ordered string within each file
		field01 = GenerateOrderedString(recordCount, maxRecords)
	} else if partitioned { // Give field ordered prefix but random string following
		prefix := GenerateOrderedString(fileCount, maxFiles)
		field01 = prefix + RandStringBytes(length-len(prefix))
	} else { // Give field random string
		field01 = RandStringBytes(length)
	}

	if len(field01) < length {
		field01 = field01 + RandStringBytes(length-len(field01))
	}
	return field01
}

func GenerateRecord(sorted bool, fileCount int, maxFiles int, recordCount int, maxRecords int, partitioned bool) TblRecord {

	field01 := GenerateFirstPrimaryKeyColumn(sorted, partitioned, fileCount, maxFiles, recordCount, maxRecords, 15)

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

func GenerateOrderedString(cnt int, max int) string {

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

var (
	cloudStorageClientOnce sync.Once
	cloudStorageClient     *storage.Client
)

// It is recommended to only use a single cloud storage client across goroutines
func getCloudStorageClient(ctx context.Context) *storage.Client {
	var err error
	cloudStorageClientOnce.Do(func() {
		cloudStorageClient, err = storage.NewClient(ctx)
		if err != nil {
			log.Fatal(err)
		}
	})
	return cloudStorageClient
}

func WriteRecords(fileCount int, maxFiles int, recordsPerFile int, storageBucket string, bucketPrefix string, sorted bool, localDirectory string, partitioned bool) {

	var encoders []*ocf.Encoder

	// Open local file
	if len(localDirectory) > 0 {
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

		encoders = append(encoders, encLocal)
	}

	if len(storageBucket) > 0 {
		// Open gcs file
		cloudFilePath := CloudFilePath(fileCount)
		ctx := context.Background()
		client := getCloudStorageClient(ctx)
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

		encoders = append(encoders, encCloud)

	}
	recordCount := 0
	// Run until we get to the file size
	start := time.Now().Unix()
	for {

		recordCount++

		record := GenerateRecord(sorted, fileCount, maxFiles, recordCount, recordsPerFile, partitioned)

		for _, encoder := range encoders {
			err := encoder.Encode(record)
			if err != nil {
				log.Fatal(err)
			}
		}

		// Print rate
		if recordCount%100000 == 0 || recordCount >= recordsPerFile {
			elapsed := time.Now().Unix() - start
			if elapsed > 0 {
				log.Printf("File %d: %d rows/sec\n", fileCount, int64(recordCount)/elapsed)
			}
		}

		if recordCount >= recordsPerFile {
			return
		}

	}
}

func GenerateAvroFiles(numFile int, recordsPerFile int, storageBucket string, bucketPrefix string, sorted bool, localDirectory string, concurrency int, partitioned bool) {

	var wg sync.WaitGroup
	rand.Seed(time.Now().UnixNano())
	fmt.Printf("Iterating from 1 to %v\n", numFile)

	// Setup a wait group to wait until all files are written
	wg.Add(numFile)

	// Create a channel that limits the number of concurrent file creations
	guard := make(chan struct{}, concurrency)

	// Loop over files
	for i := 1; i <= numFile; i++ {
		guard <- struct{}{} // add struct to channel limiter
		go func(n int) {
			WriteRecords(n, numFile, recordsPerFile, storageBucket, bucketPrefix, sorted, localDirectory, partitioned)
			wg.Done()
			<-guard
		}(i)
	}

	wg.Wait()

}
