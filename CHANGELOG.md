# Changelog

The section of a release is used as the description of its GitHub release.

## Unreleased

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
