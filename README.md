# Avro Generator Tools

Generate and examine Avro data sets! This tool is intended to help with generating
Avro test data for CockroachDB IMPORT.

## Commands

```
avrogen create [path to schema] \
  --files [number of files] \
  --size [file size in MiB] \
  [--bucket GCSBUCKET]
  [--path PATH]
  [--sorted]
```

Example:

`avrogen generate /path/to/schema --files 100 --size 250 --sorted`