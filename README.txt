Ultimate codebase for distributed system researching in Rust.

This codebase consists of following things.

A universal event-driven abstraction, i.e. `SendEvent` and `OnEvent`, that unifies protocol implementations.

Wrapping layers for various libraries to deliver messages and timeouts, perform cryptographic operations, etc.

Bridge the gaps between the abstracted protocols and wrapping layers with combinators. For example, replication protocols usually work with network models that deliver structural mesasges to indexed destination. In order to support it with TCP/UDP network, we use one combinator to translate replica indexes into socket addresses, and then use another combinator to serialize messages into raw bytes.

Finally, for the cases that the boilerblate cannot be modeled into combinators, e.g. consolidating multiple `impl OnEvent`s into systems for benchmarking or testing, provide showcases for reference and templating.

The codebase contains selected dependencies that are verified to work well with each other, and demonstrates best practice of constructing software for various purpose, including:
* concurrency manager based on `tokio` 
* IO provider: `tokio`
* timer service provider: `tokio` and a baseline implementation
* `anyhow` as error solution
* `bincode` as (de)serialization solution (with occasionally used `serde_json`)
* std `Hash`-based digest solution and `sha2` as digest library
* `secp256k1` as cryptographic library
* `axum`-based HTTP server as standalone, command line argument free evaluation artifact
* `reqwest`-based control plane
* `terraform`-based cluster/infrastracture management
* `tracing` as logging solution

The codebase is intentionally published under restrictly license, as it's intended for non-commercial use and all forkings are expected to be contributed back to upstream eventually. Besides it is inherently never production ready because e.g. breaking changes happen all the time.

TODO list all implemented protocols and applications.