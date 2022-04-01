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
  "name": "benchmark",
  "fields": [
    {
      "name": "handle",
      "type": "string"
    },
    {
      "name": "advertiseraccountid",
      "type": "int"
    },
    {
      "name": "earnedstatus",
      "type": "int"
    },
    {
      "name": "ecproduct",
      "type": "int"
    },
    {
      "name": "displaylocation",
      "type": "int"
    },
    {
      "name": "nativeengagement",
      "type": "int"
    },
    {
      "name": "rainbird",
      "type": "int"
    },
    {
      "name": "aggregatedengagementtype",
      "type": "int"
    },
    {
      "name": "billingstatus",
      "type": "int"
    },
    {
      "name": "chargeby",
      "type": "int"
    },
    {
      "name": "bidtype",
      "type": "int"
    },
    {
      "name": "cardtype",
      "type": "int"
    },
    {
      "name": "creativetype",
      "type": "int"
    },
    {
      "name": "countrycode",
      "type": "string"
    },
    {
      "name": "trendtype",
      "type": "int"
    },
    {
      "name": "timestamp",
      "type": "string"
    },
    {
      "name": "isamplifiedsponsorship",
      "type": "boolean"
    },
    {
      "name": "isamplifiedpreroll",
      "type": "boolean"
    },
    {
      "name": "count_",
      "type": "int"
    },
    {
      "name": "sum_localcost",
      "type": "int"
    },
    {
      "name": "sum_usdcost",
      "type": "int"
    },
    {
      "name": "zetasketchhll_campaignid",
      "type": "bytes"
    },
    {
      "name": "_setvaluebitmap",
      "type": "int"
    },
    {
      "name": "_batchid",
      "type": "int"
    },
    {
      "name": "_batchtype",
      "type": "int"
    },
    {
      "name": "_batchend",
      "type": "string"
    }
  ]
}`

const letters = "abcdefghijklmnopqrstuvwxyz"
const letterCnt = len(letters)

type BenchmarkRecord struct {
	Handle                   string `avro:"handle"`
	Advertiseraccountid      int    `avro:"advertiseraccountid"`
	Earnedstatus             int    `avro:"earnedstatus"`
	Ecproduct                int    `avro:"ecproduct"`
	Displaylocation          int    `avro:"displaylocation"`
	Nativeengagement         int    `avro:"nativeengagement"`
	Rainbird                 int    `avro:"rainbird"`
	Aggregatedengagementtype int    `avro:"aggregatedengagementtype"`
	Billingstatus            int    `avro:"billingstatus"`
	Chargeby                 int    `avro:"chargeby"`
	Bidtype                  int    `avro:"bidtype"`
	Cardtype                 int    `avro:"cardtype"`
	Creativetype             int    `avro:"creativetype"`
	Countrycode              string `avro:"countrycode"`
	Trendtype                int    `avro:"trendtype"`
	Timestamp                string `avro:"timestamp"`
	Isamplifiedsponsorship   bool   `avro:"isamplifiedsponsorship"`
	Isamplifiedpreroll       bool   `avro:"isamplifiedpreroll"`
	Count                    int    `avro:"count_"`
	SumLocalcost             int    `avro:"sum_localcost"`
	SumUsdcost               int    `avro:"sum_usdcost"`
	ZetasketchhllCampaignid  []byte `avro:"zetasketchhll_campaignid"`
	Setvaluebitmap           int    `avro:"_setvaluebitmap"`
	Batchid                  int    `avro:"_batchid"`
	Batchtype                int    `avro:"_batchtype"`
	Batchend                 string `avro:"_batchend"`
}

func GenerateRecord(sorted bool, fileCount int, maxFiles int, recordCount int, maxRecords int) BenchmarkRecord {

	handle := RandStringBytes(10)
	if sorted {
		handle = GenerateOrderedHandle(fileCount, maxFiles) + GenerateOrderedHandle(recordCount, maxRecords)
	}

	return BenchmarkRecord{
		Handle:                   handle,
		Advertiseraccountid:      rand.Int(),
		Earnedstatus:             rand.Int(),
		Ecproduct:                rand.Int(),
		Displaylocation:          rand.Int(),
		Nativeengagement:         rand.Int(),
		Rainbird:                 rand.Int(),
		Aggregatedengagementtype: rand.Int(),
		Billingstatus:            rand.Int(),
		Chargeby:                 rand.Int(),
		Bidtype:                  rand.Int(),
		Cardtype:                 rand.Int(),
		Creativetype:             rand.Int(),
		Countrycode:              RandStringBytes(10),
		Trendtype:                rand.Int(),
		Timestamp:                time.Now().Format(time.RFC3339),
		Isamplifiedsponsorship:   rand.Intn(2) == 1,
		Isamplifiedpreroll:       rand.Intn(2) == 1,
		Count:                    rand.Int(),
		SumLocalcost:             rand.Int(),
		SumUsdcost:               rand.Int(),
		ZetasketchhllCampaignid:  []byte(RandStringBytes(10)),
		Setvaluebitmap:           rand.Int(),
		Batchid:                  rand.Int(),
		Batchtype:                rand.Int(),
		Batchend:                 time.Now().Format(time.RFC3339),
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
		var record BenchmarkRecord
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
	return fmt.Sprintf("%s/benchmark-%05d.avro", localDirectory, fileCounter)
}

func CloudFilePath(fileCounter int) string {
	return fmt.Sprintf("benchmark-%05d.avro", fileCounter)
}

func GenerateOrderedHandle(cnt int, max int) string {

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
