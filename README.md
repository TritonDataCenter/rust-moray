# rust-moray

This is a rust implementation of a client for Joyent's
[Moray](https://github.com/joyent/moray) key-value store.

This crate includes:

* client library interface
* `listbuckets`, An example Moray Client that executes a `listBuckets` RPC method.


**Note: This crate and its interfaces are unstable and will likely change underneath you without notice.**

# Build
```
cargo build
```

# Run Examples
```
cargo run --example listbuckets
```

# Development
## Testing
```
cargo test
```

or

```
cargo test -- --nocapture
```

## Committing
Before commit, ensure that the following command is run:
```
cargo fmt
```
