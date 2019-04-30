**Note: This crate and its interfaces are still unstable and will likely change underneath you without notice.**

# rust-moray

This is a rust implementation of a client for Joyent's
[Moray](https://github.com/joyent/moray) key-value store.

This crate includes:

* moray client library interface with the following methods:
    * `list_buckets`
    * `get_buckets`
    * `create_buckets`
    * `put_object`
    * `get_object`
    * `find_objects`
    * `sql`: Raw sql interface


# Build
```
cargo build
```

# Run Examples
```
cargo run --example <listbuckets|createbucket|putobject|findobjects|sql>
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
