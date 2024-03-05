pub mod app;
pub mod bulk;
pub mod crypto;
pub mod event;
pub mod kademlia;
pub mod message;
pub mod net;
pub mod pbft;
pub mod search;
pub mod unreplicated;
pub mod worker;
pub mod workload;

// develop notes that does not apply to any specific code
// (start writing dev docs usually follows by a complete code rewriting, hope
// not the case this time)
//
// there has been a lot of generic around this codebase, naming convention
// is roughly: `M`/`N` for messages/events, `S`/`U` for state machine (that
// ususally `impl OnEvent`) (can also use `T` when it is not confused with
// representing general types), `N`/`M` for nets, `E` for `impl SendEvent`
// (i.e. emitter), `A`/`B` for addresses
// (yeah, i also just realized the unfortunate reusing of `M`/`N`)
//
// out of personal preference, code working in asynchronous context e.g.
// `async fn` are named "session". those code are mostly backed by Tokio if it
// is not reactor-agnostic. i will probably never introduce a second
// asynchronous reactor library, so "session" = asynchronous = Tokio in this
// codebase
//
// by the way, the synchronous counterpart is under modules named "blocking".
// no particular reason, just a "std" module causes unnecessary problems
//
// although does not look like so, but the codebase does not have a tendency of
// using generic/trait ploymophism everywhere. enum polymophism is used as long
// as the case is simple enough to be supported by it, e.g. crypto, worker, etc.
// the user of this codebase is generally assumed to keep this codebase at a
// writable place (i.e. not as a git/crates.io dependency), so more enum
// variants can always be added if necessary.
//
// one case that must use trait is when type erasuring is required, that is, the
// type parameters of implementors must not be exposed to the use-site. this is
// the case for e.g. `SendEvent`.
//
// for `SendMessage` it's more about code organization. if it is instead an enum
// then another enum must be constructed to hold all message types. having that
// gaint enum feels ridiculous. a `SendMessage` trait also brings additional
// benefits like allow me to come out with fancy things like `SendAddr` :)
//
// the rational for the address type generic (notice that the type is not
// necessarily `impl Addr`) is that most components only work with one address
// type (plus some auxiliry types like `All`), so an address enum will introduce
// many runtime checks and places that bugs may sneak in. it is anonying to keep
// a generic address type all over the place, but i have to choose between
// either this or anonying downcasts every time the address is evaluated. i
// decide to go down with the type parameter since it make better use of the
// compiler
//
// when working with these traits, either to keep type parameters or make them
// into trait objects (if possible) is mostly a matter of taste. there are
// certain cases where a type parameter must be introduced e.g. when it is
// propagated across interfaces, and ther are also other cases where a trait
// object must be used e.g. when sending events in circle while working with
// type-erasured events (so the `impl OnEvent` type appears on `impl SendEvent`
// instead of the event types), and trait objects must be used somewhere to
// break infinite type recursion. tentatively the rule of thumb is to allocate
// type parameters for the types that hold nonvolatile states e.g. the
// `impl App` state machines
//
// in this codebase there's no pursuit on project-level code organization for
// software engineering or whatever reason. ideally there suppose to be one big
// `src/` that every single line of code lives in. (until it spends whole night
// to finish compiling.)
// T_{`cargo build` after no-op update e.g. editing this note} = 9.23s
//
// the reason to have a separate `tools` is because those code are for distinct
// purpose. the main part of this codebase is distributed code, that should be
// packed into single executable, rsync to all over the world, and keep running
// until crashed. on the other hand the code in `tools` are for orchestrating
// evaluations. they stay in local machine and not go anywhere. they
// self-terminates. they behave as clients instead of servers, or just as simple
// scripts. there's no dependency from `tools/` to `src/`. there's no dependency
// from `src/` to `tools/` except some shared serialization schemas. the
// interfaces between them are built based on HTTP. i believe this isolated
// design results in good separation of conern
//
// for `crates` the story is much simpler. `src/` should be compilable against
// plain Ubuntu system. and code live in `crates` if they have further system
// requirements. for example, currently there's `crates/entropy` that requires
// cmake, llvm-dev, libclang-dev and clang to be present. there may be another
// crate for dpdk stuff later
//
// well, not particular useful note, but this codebase can make so much use of
// trait alias. if some day i irrationally switch to nightly toolchain, this
// must be a huge reason for it
//
// previously there's essential difference between `SendEvent` and
// `SendMessage`: the `Addr` was an associated type. after i realize that the
// address may not be unique for certain implementations, `SendMessage<A, M>`
// is almost equivalent as `SendEvent<(A, M)>`. this is exactly what certain
// net "stubs" e.g. `PeerNet` are doing
//
// i choose to not build universal connection between them because they are
// expected to be used in different context: `SendEvent` probably sends to local
// receipant i.e. `impl OnEvent`, thus it's possible to work with type-erased
// events; `SendMessage` on contrast sends to "the outside world", so the
// sending cannot be directly hooked up with the consuming, so type erasure
// cannot be performed and message types are probably plain old types. also,
// `impl SendEvent` guarantees reliable  delivery, while it's ok for
// `impl SendMessage` to silently fail
//
// i feel sorry to have so many `anyhow::Result<()>` all over the codebase. i
// should have aliased it if i foreseed it would be this many. nevertheless, i
// guess the enhancement of std error will not get stablized in any time soon,
// so there still a long time before i need to consider replace these results
// with `Result<(), Box<dyn Error + Send + Sync>>`. probably the codebase has
// been long gone before that
//
// the error rules are roughly:
//   use panic when it suppose to not panic. it's kind of in the same situation
//   of unsafe in this codebase: i know and guarantee it is safe/not going to
//   panic, but cannot convince compiler because of the lack of expressiveness,
//   so i assert it to compiler. one decent example is to "pop" a random entry
//   from hash map
//     if let Some(k) = m.keys().next() { let v = m.remove(k).unwrap(); }
//   another is to turn non-zero integer literal into NonZero* type. putting
//   panic in the same place of unsafe is aligned to the fact that there's also
//   panic assertion in heap allocation code path (at least in glibc), which
//   means some cases of failing to fulfilling an unsafe contract are already
//   converted into panicking, or in another direction, encountering a panic
//   already may imply failing to filfull some contracts
//
//   use unstructured/ad-hoc error i.e. anyhow::anyhow!() when there's no upper
//   layer wants to handle that error, so it suppose to lead to crash. the
//   oppotunity of making such conclusion is usually rare, but in this codebase
//   i happen to get a lot of chance, since myself is the only downstream user
//   of the library part of code. for example, the structural errors that not
//   exist at the presence but would like to have are the ones for disconnected
//   channels. those are usually because of unexpected exiting that caused by
//   some other more foundamental errors, so filter them off helps focus on
//   other more meaningful errors
//
//   and lastly, create on-demand structured errors when it may be captured and
//   downcast later. in some sense unstructured error offers more information on
//   control flow: it always leads to program termination, and should never
//   happen when evaluation goes as expect, so it is preferred over structured
//   error
//
// the boundary between panic and unstructured error becomes blur: they both
// should never happen when everything goes well, but at the same time both are
// still possible to happen, so the artifacts must be prepared for both to
// happen and try their best to do reliable crashing. this is the main reason
// that i used to panic only in the past. the other reasons include panic has
// builtin backtrace support (which denimished after anyhow supports backtrace
// in stable), and panic is reported when it's happening (no matter whether or
// not get caught later), so it's very simple to reliably both crash and report
// by using panic and `panic = "abort"` in cargo config. The latter has
// disadvantage of panic messages mess up when there's multiple panic happens
// (why not locking standard error before reporting? guess that it is for the
// case where the execution panic while holding the standard error lock). also,
// `panic = "abort"` does not work well with test profile, so test cases
// behavior will diverge from expectation
//
// the ration of the crashing cases is that returning error when "oh there's
// something happened but i can do nothing about it", and panicking when "oh
// there's something happened which i though should never happen". the reason to
// use panic is to save some code that i assert will never be executed, so it's
// ok to have false positive error-returning code: that just some unreachable
// code that is too hard to be proved to be dead code. however, if my assertion
// on unreachability is incorrect and the program does panic, then i should
// replace the panic with the code that previously saved e.g. return an error
// instead.
//
// on the other hand, unstructured error defines "under what condition this code
// suppose to work". if the conidition is just temporarily unsatisfied, i can
// safely ignore this transient error; otherwise, i should think about how to
// improve the implementation to make it workable in more situations.
//
// in evaluation artifact it's not trivial to crash the server by propagating
// errors. the current solution is to log the error on server side and returns
// 500 to client to reliably crash the client. client should report which
// server goes wrong, so i know where to find the relevant log later.
// propagating complete error context may be desirable if servers cannot be
// accessed easily, but that's not my case for now
