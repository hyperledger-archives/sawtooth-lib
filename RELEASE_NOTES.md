# Release Notes

## Changes in libsawtooth 0.4.0

* Add the `protocol` module with native setting and identity protocol structs
  and update code to use these structs instead of the corresponding protobufs
* Replace internal state and database implementations with implementations
  provided by Transact and remove the `database` module
* Add a permission verifier implementation that replaces the trait in the
  `gossip` module, use it for block validation, and remove the `gossip` module
  since it is now empty
* Make minor corrections and updates to Jenkins files


## Changes in libsawtooth 0.3.0

* Update transact dependency to 0.2, which enables storing the results of
  invalid transactions as transaction receipts
