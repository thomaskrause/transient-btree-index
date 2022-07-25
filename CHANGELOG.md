# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed

- Actually insert values into the cache when reading from a
  `VariableSizeTupleFile`

## [0.5.0] - 2022-07-19

### Added

- Add efficient `swap` method to swap the values at two given keys.

## [0.4.0] - 2022-07-17

### Added

- Implement an `into_iter()` method for `BtreeIndex`.

## [0.3.0] - 2022-03-10

### Added

- For types that generate fixed length byte array when serde and bincode is used, 
  an optimized implementation can be used. Configure it in `BtreeConfig` 
  with the `fixed_key_size` and `fixed_value_size` methods.

### Fixed

- Splitting a node or moving entries to right at insertion 
  could lead to a huge number of keys being re-allocated, 
  instead of simply re-using the existing key id.
  This behavior caused the disk usage to be much larger than actually needed.

## [0.2.0] - 2022-02-18

### Changed

- More efficient insert operation by not serializing a vector of keys for the whole node.
  Instead, keys are treated like values and the B-Tree nodes only store references to them.
- **Backward incompatible**: The maximum order of a B-Tree (as per its configuration) is now
  84. This is necessary, so a node block always fits inside a memory block. You can configure
  a smaller size, but with 84 the memory page is utilized fully.
- Instead of anonymous memory mapped files, we create our own temporary files. 
  The former ones might only be written out to swap, which is a problem on
  systems with small swap sizes.

### Fixed

- The block cache could grow indefinitely.

## [0.1.0] - 2022-02-10

Initial release
