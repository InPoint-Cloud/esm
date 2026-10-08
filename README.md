# An Elasticsearch Migration Tool

Elasticsearch cross version data migration, from 1.x up to 9.x.
This is a maintained fork of [medcl/esm](https://github.com/medcl/esm).

## Features

*  Cross version migration, including 7.x to 8.x / 9.x (typeless target, settings and mappings are cleaned up automatically)
*  Analyse source and target before migrating, with `--analyse`
*  Copy index settings and mappings
*  Overwrite the index name, merge several indexes into one, unify the document type name
*  HTTP basic auth and API key auth, TLS certificate verification (`--insecure` skips it), HTTP proxies
*  Sliced scroll (Elasticsearch 5.0+) and parallel bulk workers
*  Dump an index to a local file, and load an index from a local file
*  Filter the source with a query string query, select or skip `_source` fields
*  Incremental update (add/update/delete changed records) with `--sync`. It uses a different implementation that only
   handles the ***changed*** records, and is not as fast as a full copy
*  Retries of failed requests and rejected documents, failed documents can be saved and re-imported
*  Document counts of source and target are compared after the migration, the exit code is 1 if anything failed
*  Generate testing data by randomizing the source document ids

## Install

Download the archive for your platform (linux, darwin, windows on amd64 or arm64) from the
[releases](https://github.com/InPoint-Cloud/esm/releases), ie: `esm_0.11.0_linux_amd64.tar.gz`.
`checksums.txt` of the release has the SHA-256 of every archive. `esm --version` shows the version of a binary.

To compile it yourself, Go 1.26 or newer is required:

```
go build -o esm .
```

## Quick start

Copy the index `products` from a 7.x cluster to a secured 9.x cluster, with its settings and mappings:

```
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x products --copy_settings --copy_mappings --analyse
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x products --copy_settings --copy_mappings --refresh
```

The first command only analyses the migration, the second one runs it.
`-s`/`-d` are the source and target, `-m`/`-n` their basic auth, `-x` the source indexes and `-y` the target index.

## Analyse before migrating

`--analyse` reads the source and target clusters and indexes without migrating anything. It compares the settings
and mappings of every source index with its target index, reports what will fail or behave unexpectedly, and suggests
ESM flags and Elasticsearch settings. `-d` is optional, without it only the source is analysed.

```
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x "logs-*" --copy_settings --copy_mappings --analyse
```

It reports:

* the version, data nodes, `http.max_content_length` and `indices.id_field_data.enabled` of both clusters
* per index: documents, shards, size on disk and the average document size, the differences of settings and mappings,
  ie: a field that is `long` in the source and `keyword` in the target
* problems: a disabled `_source`, closed indexes, several mapping types, `string` fields or `_all` copied to 5.x/7.x+,
  fields missing in a `dynamic: strict` target, existing documents in the target
* `-c` and `--buffer_count` for the average document size, `-w` and `--sliced_scroll_size` for large indexes,
  `-b` and `http.max_content_length` for large documents, `--shards` for primary shards above 50GB,
  `--copy_settings --copy_mappings` for missing target indexes, and `indices.id_field_data.enabled` for `--sync` on 8.x+
* the migration command with the suggested flags

The report goes to stdout, the log to stderr. The exit code is 0 when the migration can run, 2 when the analysis
found an `ERROR` that makes it fail (ie: in CI before a migration), and 1 when a cluster can not be reached or no
source index matches.

The average document size is estimated from the size of the primary shards on disk, which is compressed:
the documents sent over the network are usually larger.

## Examples

### Copy indexes

copy index `index_name` from `localhost:9200` to `localhost:9201`

```
./esm -s http://localhost:9200 -d http://localhost:9201 -x index_name -w=5 -b=10 -c 10000
```

copy index `src_index` to `dest_index` on the same cluster

```
./esm -s http://localhost:9200 -d http://localhost:9200 -x src_index -y dest_index -w=5 -b=100
```

copy settings and mappings, delete and recreate the target index, filter the source with a query, refresh after the migration

```
./esm -s http://localhost:9200 -x "src_index" -q=query:phone -y "dest_index" -d http://localhost:9201 -c 10000 --shards=5 --copy_settings --copy_mappings --force --refresh
```

use sliced scroll (5.0+) to speed up reading, and change the number of shards

```
./esm -s http://localhost:9200 -d http://localhost:9201 -n elastic:passwd -f --copy_settings --copy_mappings -x bestbuykaggle --sliced_scroll_size=5 --shards=50 --refresh
```

migrate 5.x to 6.x and unify all the types to `doc`

```
./esm -s http://localhost:9200 -x "source_index*" -u "doc" -w 10 -b 10 -t "10m" -d https://localhost:9201 -m elastic:passwd -n elastic:passwd -c 5000
```

### Authentication, TLS and proxies

basic auth of the source (`-m`) and the target (`-n`)

```
./esm -s http://localhost:9200 -m elastic:passwd -x "src_index" -y "dest_index" -d http://localhost:9201 -n elastic:passwd
```

use an API key instead of basic auth, the key is the base64 `encoded` value returned by `POST /_security/api_key`

```
./esm -s http://localhost:9200 -d https://localhost:9201 --dest_api_key "<encoded_api_key>" -x products --copy_settings --copy_mappings
```

TLS certificates are verified. If a cluster uses a self-signed certificate (the default of a new 8.x/9.x install),
add its CA to the system trust store, or skip the verification with `--insecure`

```
./esm -s https://localhost:9200 -m elastic:passwd -d https://localhost:9201 -n elastic:passwd -x products --insecure
```

send the requests to the target through a proxy (`--source_proxy` for the source)

```
./esm -d https://localhost:9201 -y "dest_index" -n elastic:passwd -c 5000 -b 1 --refresh -i dump.bin --dest_proxy=http://localhost:8080
```

### Filter the source

filter with a range query

```
./esm -s https://localhost:9200 -m elastic:password -o json.out -x kibana_sample_data_ecommerce -q "order_date:[2020-02-01T21:59:02+00:00 TO 2020-03-01T21:59:02+00:00]"
```

range query on a keyword field, with escaped quotes

```
./esm -s https://localhost:9200 -m elastic:passwd -o 1.txt -x test1 -q "@timestamp.keyword:[\"2021-01-17 03:41:20\" TO \"2021-03-17 03:41:20\"]"
```

select source fields (`--skip` removes fields instead)

```
./esm -s http://localhost:9201 -x my_index -o dump.json --fields=author,title
```

### Dump to and load from files

dump documents into a local file, one JSON document per line

```
./esm -s http://localhost:9200 -x "src_index" -m elastic:passwd -c 5000 -q=query:mixer -o=dump.bin
```

load documents from a dump file into another cluster

```
./esm -d http://localhost:9200 -y "dest_index" -n elastic:passwd -c 5000 -b 5 --refresh -i=dump.bin
```

dump the source and target index sorted by `_id` and compare them, to find the differences quickly

```
./esm --sort=_id -s http://localhost:9200 -x "src_index" --truncate_output -o=src.json
./esm --sort=_id -s http://localhost:9200 -x "dst_index" --truncate_output -o=dst.json
diff -W 200 -y --suppress-common-lines <(jq -c 'del(._index)' src.json) <(jq -c 'del(._index)' dst.json)
```

generate testing data, if `input.json` contains 10 documents, the following command indexes 100 documents

```
./esm -i input.json -d http://localhost:9201 -y target-index1 --regenerate_id --repeat_times=10
```

### Incremental sync

`--sync` scrolls the source and target index side by side, sorted by `_id`, and indexes new documents and updates
changed ones. It copies one source index to one target index.

```
./esm --sync -s http://localhost:9200 -d http://localhost:9201 -x src_index -y dest_index
```

* `--dry` only prints what would be indexed, updated or deleted
* `--enable_delete` deletes documents of the target that are not in the source, without it they are only counted
* `--ignore_content_compare` only compares the ids, documents that exist on both sides are not updated
* on an 8.x/9.x cluster sorting by `_id` needs `indices.id_field_data.enabled`, see [Migrating to 8.x / 9.x](#migrating-to-8x--9x)

`--diff_counts` prints the indexes whose document counts are equal, differ, or are missing in the target.

### Logging and profiling

The log goes to stderr, `-v` sets the level (`trace`, `debug`, `info`, `warn`, `error`), `--log_file` also writes it
to a file, and `--pprof` starts a profiling server on 127.0.0.1:6060 (or `--pprof=<address>`).

## Migrating to 8.x / 9.x

ESM detects the target version and handles the differences of 8.x and 9.x automatically:

* documents are written without `_type`, mapping types are removed in 8.x.
* documents of system indices (starting with `.`) are skipped, unless `-a` or `-y` is used.
* with `--copy_settings`, settings that 8.x/9.x reject are removed: `index.mapper.dynamic`, `max_adjacency_matrix_filters`,
  `force_memory_term_dictionary`, `soft_deletes.enabled`, `translog.retention`, `frozen`, `search.throttled`,
  `verified_before_close`, `resize`, `shrink`, `routing.allocation.initial_recovery`.
  The deprecated `nGram` / `edgeNGram` analysis types are renamed to `ngram` / `edge_ngram`.
* with `--copy_mappings`, `_field_names.enabled` and the `boost` parameter (fields, multi-fields, dynamic templates) are removed,
  `dynamic_templates` are kept.
* copying mappings across major versions is supported from 7.x and above, for older versions ESM only warns and the
  mappings should be checked manually. `--analyse` shows the differences.
* a secured cluster needs `-m`/`-n` or `--source_api_key`/`--dest_api_key`, otherwise ESM stops with an authentication error.
* `--sync` and `--sort=_id` sort by `_id`, which is disabled by default since 8.0. Enable it on the 8.x/9.x clusters first:

```
PUT _cluster/settings
{"persistent": {"indices.id_field_data.enabled": true}}
```

## Performance

ESM is fast, a 3 nodes cluster (3 * c5d.4xlarge, 16C, 32GB, 10Gbps) migrated 10,000,000 documents within a minute:

```
./esm -s https://localhost:8000 -d https://localhost:8000 -x logs1kw -y logs122 -m elastic:passwd -n elastic:passwd -w 40 --sliced_scroll_size=60 -b 5 --buffer_count=2000000 --regenerate_id
```

The options that matter most:

* `-w` the number of bulk workers writing to the target
* `--sliced_scroll_size` the number of parallel scrolls reading from the source (5.x+)
* `-c` the number of documents per scroll page, `-b` the bulk request size in MB
* `--buffer_count` the number of documents buffered in memory between reading and writing

`--analyse` suggests values for them based on the size of the indexes and documents.

The scroll is sorted by `_doc` by default, the fastest order. Avoid `--sort=_id` on large indexes,
it loads all ids into the heap of the source cluster.

### Target index settings

With `--copy_settings`, ESM creates the target index with the number of shards of the source (or `--shards`),
sets `number_of_replicas` to `0` and `refresh_interval` to `-1` during the copy, and restores them from the source afterwards.

If you create the target index yourself, use similar settings for the copy and restore them afterwards, ie:

```
PUT your-new-index
{
  "settings": {
    "index.translog.durability": "async",
    "refresh_interval": "-1",
    "number_of_shards": 10,
    "number_of_replicas": 0
  }
}
```

### Large documents

Large documents (ie: 20MB each) need smaller pages and buffers, the source holds a scroll page in its heap
and ESM holds up to `--buffer_count` documents in memory:

```
./esm -s http://localhost:9200 -d http://localhost:9201 -x files -c 2 --buffer_count=50 -w 2
```

A single document can not be larger than `http.max_content_length` of the target (100MB by default).

## Failures, retries and exit codes

* every bulk response is checked, documents rejected by the target (ie: a mapping conflict) are counted as failed,
  the first 10 errors are logged and the progress bar only counts indexed documents.
* bulk and scroll requests are retried up to `--max_retries` times (1s, 2s, 4s, ... up to 30s) on 429/502/503/504 and network errors,
  documents rejected with 429 (`es_rejected_execution_exception`) are retried alone.
* at the end ESM prints `documents sent / indexed / failed`, refreshes the target indexes and compares `_count`
  of every source index (with `-q` applied) and its target index (respecting `-y`); disable it with `--skip_count_check`.
  The check is skipped when reading from or writing to a file.
* with `--failed_output`, every document that still failed after the retries is appended to a file in the dump format
  (`_index` is the target index name), fix the cause (ie: the mapping) and re-import them:

```
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x products --failed_output=failed.json
./esm -i failed.json -d https://localhost:9201 -n elastic:passwd
```

Exit codes, so ESM can be used in scripts:

Exit code | Meaning
----------|--------
0 | the migration (or sync) succeeded, `--analyse` found no error
1 | a document failed, reading from the source failed, copying settings/mappings failed, the counts differ, a cluster can not be reached, or the arguments are invalid
2 | `--analyse` found an `ERROR` that makes the migration fail

## Supported versions

From | To
-----|---
1.x, 2.x, 5.x, 6.x, 7.x | 1.x, 2.x, 5.x, 6.x, 7.x
7.x | 8.x, 9.x
8.x | 8.x, 9.x
9.x | 9.x

The integration tests run 7.17 to 8.19 and 8.19 to 9.5 (with security and a self-signed certificate) on every push.

## Options

```
Usage:
  esm [OPTIONS]

Application Options:
  -s, --source=                    source elasticsearch instance, ie:
                                   http://localhost:9200
  -q, --query=                     query against source elasticsearch instance,
                                   filter data before migrate, ie: name:john
      --sort=                      sort field when scroll, ie: _id (default:
                                   _doc, the fastest order, --sync uses _id)
  -d, --dest=                      destination elasticsearch instance, ie:
                                   http://localhost:9201
  -m, --source_auth=               basic auth of source elasticsearch instance,
                                   ie: user:pass
  -n, --dest_auth=                 basic auth of target elasticsearch instance,
                                   ie: user:pass
      --source_api_key=            api key of source elasticsearch instance
                                   (base64 encoded id:api_key), used instead of
                                   source_auth
      --dest_api_key=              api key of target elasticsearch instance
                                   (base64 encoded id:api_key), used instead of
                                   dest_auth
  -c, --count=                     number of documents at a time: ie "size" in
                                   the scroll request (default: 10000)
      --buffer_count=              number of buffered documents in memory
                                   (default: 1000000)
  -w, --workers=                   concurrency number for bulk workers
                                   (default: 1)
  -b, --bulk_size=                 bulk size in MB (default: 5)
  -t, --time=                      scroll time (default: 10m)
      --sliced_scroll_size=        size of sliced scroll, to make it work, the
                                   size should be > 1 (default: 1)
  -f, --force                      delete destination index before copying
  -a, --all                        copy indexes starting with . and _
      --copy_settings              copy index settings from source
      --copy_mappings              copy index mappings from source
      --shards=                    set a number of shards on newly created
                                   indexes
  -x, --src_indexes=               indexes name to copy,support regex and comma
                                   separated list (default: _all)
  -y, --dest_index=                indexes name to save, allow only one
                                   indexname, original indexname will be used
                                   if not specified
  -u, --type_override=             override type name
      --green                      wait for both hosts cluster status to be
                                   green before dump. otherwise yellow is okay
  -v, --log=                       setting log
                                   level,options:trace,debug,info,warn,error
                                   (default: INFO)
      --log_file=                  also write the log to this file
      --insecure                   skip verifying the TLS certificates of
                                   source and target, ie: for self-signed
                                   certificates
      --pprof=                     start a pprof server, on 127.0.0.1:6060 or
                                   the given address
  -o, --output_file=               output documents of source index into local
                                   file
      --truncate_output            truncate before dump to output file
  -i, --input_file=                indexing from local dump file
      --source_proxy=              set proxy to source http connections, ie:
                                   http://127.0.0.1:8080
      --dest_proxy=                set proxy to target http connections, ie:
                                   http://127.0.0.1:8080
      --refresh                    refresh after migration finished
      --sync                       sync will use scroll for both source and
                                   target index, compare the data and
                                   sync(index/update/delete)
      --fields=                    filter source fields(white list), comma
                                   separated, ie: col1,col2,col3,...
      --skip=                      skip source fields(black list), comma
                                   separated, ie: col1,col2,col3,...
      --repeat_times=              repeat the data from source N times to dest
                                   output, use align with parameter
                                   regenerate_id to amplify the data size
  -r, --regenerate_id              regenerate id for documents, this will
                                   override the exist document id in data source
  -p, --sleep=                     sleep N seconds after each bulk request
                                   (default: -1)
      --diff_counts                count the difference between source and
                                   target indexes
      --analyse                    analyse source and target without migrating:
                                   compare the settings and mappings of the
                                   indexes, suggest esm flags and elasticsearch
                                   settings
      --remain_routing_allocation  keep routing allocation in mappings
      --only_meta                  only sync meta
      --dry                        only dry
      --enable_delete              enable delete records in dest index if there
                                   are more records
      --ignore_content_compare     ignore to compare the content of a record
      --max_retries=               max retries of a failed bulk or scroll
                                   request (429/502/503/504 or network error),
                                   with exponential backoff (default: 5)
      --failed_output=             append documents that failed to index to
                                   this file, it can be re-imported with -i
      --skip_count_check           skip comparing the document counts of source
                                   and target indexes after migration
      --version                    print the esm version and exit

Help Options:
  -h, --help                       Show this help message
```


## FAQ

- Scroll ID too long: update `elasticsearch.yml` on the source cluster.

```
http.max_header_size: 16k
http.max_initial_line_length: 8k
```

## Development

### Testing

`go test ./...` runs the unit tests and the end to end tests against an in-memory fake elasticsearch, in about a second.
The integration tests run against real clusters in containers (podman or docker):

```
scripts/integration-test.sh 7.17.28 8.19.23          # source version, target version
scripts/integration-test.sh 8.19.23 9.5.5 secure     # target with security, https and a self-signed certificate
```

CI runs both on every push and pull request.

### Releasing

Releases are built by GoReleaser in GitHub Actions (`.github/workflows/Release.yml`).
Push a semver tag and the workflow runs the tests, builds linux/darwin/windows binaries for amd64 and arm64,
then publishes them with checksums and release notes as a GitHub release:

```
git tag v1.0.0
git push origin v1.0.0
```

Before tagging, rename the `## Unreleased` section of `CHANGELOG.md` to the tag (ie: `## v1.0.0`),
it becomes the description of the release. Without a section GoReleaser lists the commits since the last tag.

Tags with a suffix such as `v1.1.0-rc.1` are published as pre-releases. Running the workflow
manually builds a snapshot and attaches the binaries to the workflow run instead of releasing.
To try it locally: `goreleaser release --snapshot --clean`.

## Links

- [Dec 3rd, 2020: [EN] Cross version Elasticsearch data migration with ESM](https://discuss.elastic.co/t/dec-3rd-2020-en-cross-version-elasticsearch-data-migration-with-esm/256516)
