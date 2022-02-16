# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- More efficient insert operation by not serializing a vector of keys for the whole node.
  Instead, keys are treated like values and the B-Tree nodes only store references to them.
- **Backward incompatible**: The maximum order of a B-Tree (as per its configuration) is now
  84. This is necessary, so a node block always fits inside a memory block. You can configure
  a smaller size, but with 84 the memory page is utilized fully.

### Fixed

- The block cache could grow indefinitely.

## [0.1.0] - 2022-02-10

Initial release