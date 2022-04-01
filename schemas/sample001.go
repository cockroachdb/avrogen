package schemas

import "time"

const sample001Schema = `{
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

type Sample001Record struct {
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

func GenerateRecord(sorted bool, fileCount int, maxFiles int, recordCount int, maxRecords int) Sample001Record {

	handle := RandStringBytes(10)
	if sorted {
		handle = GenerateOrderedHandle(fileCount, maxFiles) + GenerateOrderedHandle(recordCount, maxRecords)
	}

	return Sample001Record{
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
