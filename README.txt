Ultimate codebase for distributed system researching in Rust.

The codebase is built around a set of simple yet powerful event driven abstractions. Message passing is used for dual purposes: for the concurrency management and as the polymorphism mechanism among state machines. And yes, each protocol is implemented into an event driven state machine that fully decoupled with IO, worker thread pool, and even itself.

The codebase aims both performance and correctness. The state machine abstraction is near zero cost and compatible with highly efficiency event loops. At the same time, extensive simulation and even model checking are able to be performed against the state machines.

The codebase contains selected dependencies that are verified to work well with each other, and demonstrates a best practice of constructing artifact for evaluation of distributed system academic researching.

Finally the codebase contains plenty of implementations of previous works in a unified form, which are good baselines to compare and starting point for experimenting modifications. TODO list all implemented protocols and applications.

The codebase is intentionally published under a restricted license, as it's intended for non-commercial use and all forking are expected to be contributed back to upstream eventually. Besides it is inherently never production ready because e.g. breaking changes happen all the time.