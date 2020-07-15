# Release Notes

## Changes in libsawtooth 0.3.1

* Move Sawtooth validator Rust code that could be easily separated from the
  Python code into libsawtooth
* Add feature guard to prevent unused import warning
* Add protobuf generation to libsawtooth
* Add stable, experimental features to all crates
* Fix formatting and lint


## Changes in libsawtooth 0.3.0

* Update transact dependency to 0.2, which enables storing the results of
  invalid transactions as transaction receipts
