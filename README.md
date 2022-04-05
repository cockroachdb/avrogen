# Avro Generator Tools

Generate and examine Avro data sets! This tool is intended to help with generating
Avro test data for CockroachDB IMPORT.

## Commands

```
avrogen create \
  --files [number of files] \
  --size [number of rows to generate per file] \
  [--bucket GCSBUCKET]
  [--bucket-path PATH]
  [--local-path]
  [--sorted]
  [--partitioned]
  [--concurrency CONCURRENCY]
```

Note:

`sorted` ensures that rows are sorted within a single file

`partitioned` ensures that files do not have overlapping data (data is sorted across files)

Example:

`avrogen generate /path/to/schema --files 100 --size 10000 --sorted --local-path /tmp`

To save to local directory, use the `--local-path` flag.

To save to a GCS bucket, use the `--bucket` and `--bucket-path` flags.
## Test data

Avrogen comes with a test schema that is derived from a customer workload.

Each row is approximately 181 bytes.

```
10k = 1.7 MiB
100k = 17 MiB
1M = 173 MiB
10M = 1.73 GiB
100M = 17.3 GiB
1B = 173 GiB
etc
```

## Build for Linux

```
GOOS=linux GOARCH=amd64 go build -o bin/avrogen
```