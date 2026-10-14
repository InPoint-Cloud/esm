# Changelog

The section of a release is used as the description of its GitHub release.

## Unreleased

### Changed
- The wait before a migration checks the health of the migrated indexes (`_cluster/health/<index>`) instead
  of the whole cluster, so a red index that is not migrated no longer blocks the migration forever. Without
  `--dest_index` the source index names are checked on the target, a target index that does not exist yet is
  skipped. Without index names (a dump file without `--dest_index`) the whole cluster is still checked.

## v0.12.1

### Changed
- A document that is over a size limit of the target is no longer retried: a 413, an indexing pressure
  rejection (429 `es_rejected_execution_exception`) that would repeat when the document is sent on its own,
  or a circuit breaker that reports `PERMANENT` durability fails it right away and saves it to `--failed_output`.
  Elasticsearch 9 counts a primary operation at about 4 times the document size, a 50mb document needs an
  `indexing_pressure.memory.limit` of about 260mb.
- A failed document is logged with its index, id and size, and a hint names the Elasticsearch setting
  to raise (`http.max_content_length`, `indexing_pressure.memory.limit`, `indices.breaker.total.limit`).

### Fixed
- Log lines written while the progress bars were shown were partly overwritten by the next redraw of the
  bars, which left copies of the bars behind ("Read 6 / 6" many times). A log line is now printed in place
  of the bars and the bars are drawn again below it.

## v0.12.0

### Added
- `--request_timeout` (default `10m`) limits a request to Elasticsearch, a hanging connection blocked the
  migration forever. A timed out request is retried like a network error.

### Changed
- HTTP connections are kept alive and reused, every scroll and bulk request opened a new connection
  (and TLS handshake) before, which was slow and could run out of local ports on long migrations.
- Without `--green`, a red cluster is now waited for (yellow is still okay), it was ignored before.
- The Windows binaries carry version info (company, product, file version) and a manifest, an unsigned
  exe without them was flagged by Defender as Trojan:Win32/Bearfoos.B!ml (a false positive).
- Progress bars use `cheggaaa/pb/v3`, the unmaintained v1 module is no longer used.

### Fixed
- Ctrl-C (SIGINT) or SIGTERM left the target indexes with `refresh_interval: -1` and `number_of_replicas: 0`,
  the settings are now restored before esm exits with 130. A second Ctrl-C exits right away.
- esm crashed when it shortened a long error message with non-ASCII characters, ie: a bulk error quoting
  a German field value.
- `-o` to a file that could not be opened hung forever, write errors (ie: a full disk) were ignored and
  esm exited with 0. Both now exit with 1.
- `-o` with `--truncate_output` and `--repeat_times` kept only the documents of the last round.
- `--sync` with `--sliced_scroll_size` above 1 compared only the first slice, the others were not synced
  and with `--enable_delete` their target documents could be deleted. Sync now ignores the option.
- `--failed_output` was ignored with `--sync`: documents that still failed after all retries (ie: 429)
  were counted as failed but no file was written.
- `--sync` ignored `-b`/`--bulk_size` and sent all new or changed documents of a scroll page (or all
  documents left over at the end) in a single bulk request, which the target often rejected with 429.
  Sync bulk requests are now split at `--bulk_size` like a normal migration.
- A bulk request the target rejects as a whole with 429 (circuit breaker, ie: "Data too large") or 413
  is now split in two halves that are sent one after the other, instead of resending the same large
  request until the retries run out.

## v0.11.2

### Fixed
- `--analyse` converted the document count of an index to `int`, a count above 2^31 was shown wrong on
  32-bit builds (code scanning alert #1).

## v0.11.1

### Changed
- The README was reworked: install and quick start sections, `--sync` options, a table of exit codes,
  examples grouped by task, and the outdated scroll sort note fixed.

## v0.11.0

`--analyse` checks a migration before running it.

### Added
- `--analyse` reads source and target without migrating: it compares the settings and mappings of the indexes,
  reports problems (ie: field types that differ, `string` fields for 5.x+, a disabled `_source`, several mapping types)
  and suggests flags (`-c`, `--buffer_count`, `-w`, `--sliced_scroll_size`, `-b`, `--shards`, `--copy_settings`)
  and Elasticsearch settings (`http.max_content_length`, `indices.id_field_data.enabled` for `--sync` on 8.x+).
  It exits with 2 when it finds an error that makes the migration fail.

## v0.10.1

Fixes for large migrations, tested with 25MB documents and 1 million documents.

### Fixed
- The bulk workers stopped when the source sent nothing for 5 minutes, the migration could hang.
- The last document of an input file (`-i`) was dropped when the file did not end with a newline,
  read errors of the file were ignored.

### Changed
- The scroll is sorted by `_doc` instead of `_id` by default, the fastest order. Sorting by `_id` loads all ids
  into the heap of the source cluster, a problem for large indexes. `--sync` still sorts by `_id`.
- The README explains the settings for large documents (`-c`, `--buffer_count`).

## v0.10.0

### Fixed
- `--copy_mappings` without `--copy_settings` was ignored, the mappings are copied now.
- `--sync` to a 5.x/6.x target failed, the documents were sent without a mapping type.
- `--sync` exited with 0 when the source or target index could not be read.
- Migrating to 7.x failed for documents without a type, ie: from an 8.x source.
- `--copy_mappings` printed the index names to stdout.

### Removed
- `--compress`, it had no effect. Responses are still gzip compressed, Go requests that by default.

### Changed
- Tests cover all supported versions (1.x to 9.x), `--sync`, settings and mappings, auth, proxies and retries.
- Integration tests against real 7.x, 8.x and 9.x clusters (`scripts/integration-test.sh`), run by CI.
- "can not connect to the source/target elasticsearch" instead of "can not parse source/target es api".
- The code comments are in English, the README examples are updated.

## v0.9.0

### Breaking changes
- **TLS certificates are verified.** Clusters with a self-signed certificate (the default of a new 8.x/9.x install)
  need `--insecure`, or their CA in the system trust store.
- Removed flags that had no effect: `-l/--logstash_endpoint`, `--secured_logstash_endpoint`, `--input_file_type`,
  `--rename` and `--ignore_compare_fields`.
- The log goes to stderr instead of stdout, and `esm.log` is only written with `--log_file`.
  The log format is `time=… level=INFO source=file.go:12 msg="…"`.
- The pprof server only starts with `--pprof`, it was always listening on `0.0.0.0:6060` before.
- Go 1.26 is required to compile, the module path is `github.com/InPoint-Cloud/esm`.

### Added
- `--insecure`, `--log_file` and `--pprof`.

### Fixed
- Dump files ended with an empty `{}` document, re-importing them added an empty document.
- `--skip` did not remove any fields, it works for es and file outputs now.
- A dump that failed while reading the source exited with 0.
- 5.x/6.x scroll errors were logged as `EOF` and retried even if they could not succeed (ie: an expired scroll).
- Requests could go through the proxy of the other cluster when `--source_proxy` and `--dest_proxy` were set.
- A data race between the bulk workers (`-w` > 1).
- Shard failure reasons containing `%` were garbled in the log.

### Changed
- `gorequest`, `fasthttp` and `seelog` are replaced by the standard library, fewer dependencies and no known vulnerabilities.
- CI runs `go vet` and the tests on every push and pull request.

## v0.8.0

### Added
- Releases are built automatically: pushing a `v*` tag builds linux, macOS and windows binaries (amd64, arm64)
  and publishes them with checksums as a GitHub release.
- `--version` prints the version, commit and build date.

### Fixed
- The scroll slices could finish before all documents were read, a run with a slice without documents could hang.
