# An Elasticsearch Migration Tool

Elasticsearch cross version data migration.

Links:
- [Dec 3rd, 2020: [EN] Cross version Elasticsearch data migration with ESM](https://discuss.elastic.co/t/dec-3rd-2020-en-cross-version-elasticsearch-data-migration-with-esm/256516)
- [Use INFINI Gateway to check the Document-Level differences between two clusters or indices after the migration](https://gateway.infinilabs.com/docs/tutorial/index_diff/)

## Features:

*  Cross version migration supported
*  Overwrite index name
*  Copy index settings and mapping
*  Support http basic auth and api key auth
*  Support migrating from 7.x to 8.x / 9.x (typeless target, settings and mappings are cleaned up automatically)
*  Support dump index to local file
*  Support loading index from local file
*  Support http proxy
*  Support sliced scroll ( elasticsearch 5.0 +)
*  Support run in background
*  Generate testing data by randomize the source document id
*  Support rename filed name
*  Support unify document type name
*  Support specify which _source fields to return from source
*  Support specify query string query to filter the data source
*  Support rename source fields while do bulk indexing
*  Support incremental update(add/update/delete changed records) with `--sync`. Notice: it use different implementation, just handle the ***changed*** records, but not as fast as the old way
*  Load generating with 

## ESM is fast!

A 3 nodes cluster(3 * c5d.4xlarge， 16C，32GB，10Gbps)

```
root@ip-172-31-13-181:/tmp# ./esm -s https://localhost:8000 -d https://localhost:8000 -x logs1kw -y logs122 -m elastic:medcl123 -n elastic:medcl123 -w 40 --sliced_scroll_size=60 -b 5 --buffer_count=2000000  --regenerate_id
[12-19 06:31:20] [INF] [main.go:506,main] start data migration..
Scroll 10064570 / 10064570 [=================================================] 100.00% 55s
Bulk 10062602 / 10064570 [==================================================]  99.98% 55s
[12-19 06:32:15] [INF] [main.go:537,main] data migration finished.
```
Migrated 10,000,000 documents within a minute, Nginx log generated from kibana_sample_data_logs.


## Before ESM

Before running the esm, please manually prepare the target index with mapping and optimized settings to improve the speed, for example:

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

## Example:

copy index `index_name` from `192.168.1.x` to `192.168.1.y:9200`

```
./bin/esm  -s http://192.168.1.x:9200   -d http://192.168.1.y:9200 -x index_name  -w=5 -b=10 -c 10000
```

copy index `src_index` from `192.168.1.x` to `192.168.1.y:9200` and save with `dest_index`

```
./bin/esm -s http://localhost:9200 -d http://localhost:9200 -x src_index -y dest_index -w=5 -b=100
```

use sync feature for incremental update index `src_index` from `192.168.1.x` to `192.168.1.y:9200`
```
./bin/esm --sync -s http://localhost:9200 -d http://localhost:9200 -x src_index -y dest_index
```

support Basic-Auth
```
./bin/esm -s http://localhost:9200 -x "src_index" -y "dest_index"  -d http://localhost:9201 -n admin:111111
```

copy settings and override shard size
```
./bin/esm -s http://localhost:9200 -x "src_index" -y "dest_index"  -d http://localhost:9201 -m admin:111111 -c 10000 --shards=50  --copy_settings

```

copy settings and mapping, recreate target index, add query to source fetch, refresh after migration
```
./bin/esm -s http://localhost:9200 -x "src_index" -q=query:phone -y "dest_index"  -d http://localhost:9201  -c 10000 --shards=5  --copy_settings --copy_mappings --force  --refresh

```

dump elasticsearch documents into local file
```
./bin/esm -s http://localhost:9200 -x "src_index"  -m admin:111111 -c 5000 -q=query:mixer  --refresh -o=dump.bin 
```

dump source and target index to local file and compare them, so can find the difference quickly
```
./bin/esm --sort=_id -s http://localhost:9200 -x "src_index" --truncate_output --skip=_index -o=src.json
./bin/esm --sort=_id -s http://localhost:9200 -x "dst_index" --truncate_output --skip=_index -o=dst.json
diff -W 200 -ry --suppress-common-lines src.json dst.json
```

loading data from dump files, bulk insert to another es instance
```
./bin/esm -d http://localhost:9200 -y "dest_index"   -n admin:111111 -c 5000 -b 5 --refresh -i=dump.bin
```

support proxy
```
 ./bin/esm -d http://123345.ap-northeast-1.aws.found.io:9200 -y "dest_index"   -n admin:111111  -c 5000 -b 1 --refresh  -i dump.bin  --dest_proxy=http://127.0.0.1:9743
```

use sliced scroll(only available in elasticsearch v5) to speed scroll, and update shard number
```
 ./bin/esm -s=http://192.168.3.206:9200 -d=http://localhost:9200 -n=elastic:changeme -f --copy_settings --copy_mappings -x=bestbuykaggle  --sliced_scroll_size=5 --shards=50 --refresh
```

migrate 5.x to 6.x and unify all the types to `doc`
```
./esm -s http://source_es:9200 -x "source_index*"  -u "doc" -w 10 -b 10 - -t "10m" -d https://target_es:9200 -m elastic:passwd -n elastic:passwd -c 5000 

```

filter migration with range query

```
./esm -s https://192.168.3.98:9200 -m elastic:password -o json.out -x kibana_sample_data_ecommerce -q "order_date:[2020-02-01T21:59:02+00:00 TO 2020-03-01T21:59:02+00:00]"

```

range query, keyword type and escape

```
./esm -s https://192.168.3.98:9200 -m test:123 -o 1.txt -x test1  -q "@timestamp.keyword:[\"2021-01-17 03:41:20\" TO \"2021-03-17 03:41:20\"]"
```

generate testing data, if `input.json` contains 10 documents, the follow command will ingest 100 documents, good for testing
```
./bin/esm -i input.json -d  http://localhost:9201 -y target-index1  --regenerate_id  --repeat_times=10 
```

select source fields

```
 ./bin/esm -s http://localhost:9201 -x my_index -o dump.json --fields=author,title
```

user buffer_count to control memory used by ESM， and use gzip to compress network traffic
```
./esm -s https://localhost:8000 -d https://localhost:8000 -x logs1kw -y logs122 -m elastic:medcl123 -n elastic:medcl123 --regenerate_id -w 20 --sliced_scroll_size=60 -b 5 --buffer_count=1000000 --compress false 
```

migrate from elasticsearch 7.x to 9.x (or 8.x), copy settings and mappings, the target uses https and basic auth

```
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x "products,logs" --copy_settings --copy_mappings --refresh
```

use an api key instead of basic auth, the key is the base64 `encoded` value returned by `POST /_security/api_key`

```
./esm -s http://localhost:9200 -d https://localhost:9201 --dest_api_key "<encoded_api_key>" -x products --copy_settings --copy_mappings
```

## Download
https://github.com/InPoint-Cloud/esm/releases


## Compile:
if download version is not fill you environment,you may try to compile it yourself. `go` required.

`go build -o esm .`
* go version >= 1.26

## Releasing
Releases are built by GoReleaser in GitHub Actions (`.github/workflows/Release.yml`).
Push a semver tag and the workflow builds linux/darwin/windows binaries for amd64 and arm64,
then publishes them with checksums and a changelog as a GitHub release:

```
git tag v1.0.0
git push origin v1.0.0
```

Tags with a suffix such as `v1.1.0-rc.1` are published as pre-releases. Running the workflow
manually builds a snapshot and attaches the binaries to the workflow run instead of releasing.
To try it locally: `goreleaser release --snapshot --clean`. `esm --version` shows the version of a build.

## Options

```
Usage:
  esm [OPTIONS]

Application Options:
  -s, --source=                    source elasticsearch instance, ie:
                                   http://localhost:9200
  -q, --query=                     query against source elasticsearch instance,
                                   filter data before migrate, ie: name:medcl
      --sort=                      sort field when scroll, ie: _id (default:
                                   _id)
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
      --compress                   use gzip to compress traffic
  -p, --sleep=                     sleep N seconds after each bulk request
                                   (default: -1)
      --diff_counts                count the difference between source and
                                   target indexes
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

- Scroll ID too long, update `elasticsearch.yml` on source cluster.

```
http.max_header_size: 16k
http.max_initial_line_length: 8k
```

- Failed documents, retries and the final check

  * every bulk response is checked, documents rejected by the target (ie: a mapping conflict) are counted as failed,
    the first 10 errors are logged and the progress bar only counts indexed documents.
  * bulk and scroll requests are retried up to `--max_retries` times (1s, 2s, 4s, ... up to 30s) on 429/502/503/504 and network errors,
    documents rejected with 429 (`es_rejected_execution_exception`) are retried alone.
  * at the end ESM prints `documents sent / indexed / failed`, refreshes the target indexes and compares `_count`
    of every source index (with `-q` applied) and its target index (respecting `-y`); disable it with `--skip_count_check`.
    The check is skipped when reading from or writing to a file.
  * ESM exits with status 1 if any document failed, reading from the source failed, copying settings/mappings failed,
    or the counts differ, so it can be used in scripts.
  * with `--failed_output`, every document that still failed after the retries is appended to a file in the dump format
    (`_index` is the target index name), fix the cause (ie: the mapping) and re-import them:

```
./esm -s http://localhost:9200 -d https://localhost:9201 -n elastic:passwd -x products --failed_output=failed.json
./esm -i failed.json -d https://localhost:9201 -n elastic:passwd
```
  * with `--copy_settings`, new target indexes keep the number of shards of the source index (unless `--shards` is set),
    `number_of_replicas` and `refresh_interval` are set to `0` / `-1` during the copy and restored from the source afterwards.

- Migrating to 8.x / 9.x

  ESM detects the target version and handles the differences of 8.x and 9.x automatically:

  * documents are written without `_type`, mapping types are removed in 8.x.
  * documents of system indices (starting with `.`) are skipped, unless `-a` or `-y` is used.
  * with `--copy_settings`, settings that 8.x/9.x reject are removed: `index.mapper.dynamic`, `max_adjacency_matrix_filters`,
    `force_memory_term_dictionary`, `soft_deletes.enabled`, `translog.retention`, `frozen`, `search.throttled`,
    `verified_before_close`, `resize`, `shrink`, `routing.allocation.initial_recovery`.
    The deprecated `nGram` / `edgeNGram` analysis types are renamed to `ngram` / `edge_ngram`.
  * with `--copy_mappings`, `_field_names.enabled` and the `boost` parameter (fields, multi-fields, dynamic templates) are removed,
    `dynamic_templates` are kept.
  * copying mappings across major versions is supported from 7.x and above, for older versions ESM only warns and the mappings should be checked manually.
  * a secured cluster needs `-m`/`-n` or `--source_api_key`/`--dest_api_key`, otherwise ESM stops with an authentication error.
  * when the source is 8.x+, the scroll is sorted by `_doc` instead of `_id`, because sorting on `_id` is disabled by default.
  * `--sync` sorts both sides by `_id`, so on an 8.x/9.x target enable it first:

```
PUT _cluster/settings
{"persistent": {"indices.id_field_data.enabled": true}}
```

Versions
--------

From       | To
-----------|-----------
1.x | 1.x
1.x | 2.x
1.x | 5.x
1.x | 6.x
1.x | 7.x
2.x | 1.x
2.x | 2.x
2.x | 5.x
2.x | 6.x
2.x | 7.x
5.x | 1.x
5.x | 2.x
5.x | 5.x
5.x | 6.x
5.x | 7.x
6.x | 1.x
6.x | 2.x
6.x | 5.0
6.x | 6.x
6.x | 7.x
7.x | 1.x
7.x | 2.x
7.x | 5.x
7.x | 6.x
7.x | 7.x
7.x | 8.x
7.x | 9.x
8.x | 8.x
8.x | 9.x
9.x | 9.x

