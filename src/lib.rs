pub mod app;
pub mod blob;
pub mod crypto;
pub mod event;
pub mod kademlia;
pub mod net;
pub mod pbft;
pub mod rpc;
pub mod search;
pub mod unreplicated;
pub mod worker;

// develop notes that does not apply to any specific code
// (start writing dev docs usually follows by a complete code rewriting, hope
// not the case this time)
//
// there has been a lot of generic around this codebase, naming convention
// is roughly: `M`/`N` for messages/events, `S`/`U` for state machine (that
// ususally `impl OnEvent`) (and `T` is not confused with representing general
// types), `N`/`M` for nets, `E` for `impl SendEvent` (i.e. emitter), `A`/`B`
// for addresses
// (yeah, i also just realized the awful reusing of `M`/`N`)
//
// although does not look like so, but the spirit is to avoid generic/trait
// usage as much as possible. always prefer enum polymophism over trait one.
// existing enum polymophism includes crypto, worker, and app (which is trait
// for now, but probably will be converted soon or later). the user of this
// codebase is generally assumed to keep this codebase at a writable place (i.e.
// not as a git/crates.io dependency), so more enum variants can always be added
// if necessary.
//
// one case that must use trait is when type erasuring is required, that is, the
// type parameters of implementors must not be exposed to the use-site, which
// is a requirement that enum cannot fulfill. this is the case for `SendEvent`.
//
// for `SendMessage` it's more about code organization. if it is instead an enum
// then all message types must be also made into another enum. having that gaint
// enum feels nonsense. a `SendMessage` trait also brings additional benefits
// like allow me to write down fancy `SendAddr` stuff :)
//
// the reason for `Addr` is that most components only work with one address type
// (plus some auxiliry types like `AllReplica`), so an address enum will
// introduce more runtime check and more places that bugs may sneak in. it is
// anonying to keep a generic address type everywhere, but i have to choose
// either this or anonying downcasts every time the address is evaluated. i
// decide to choose the type parameter since it make better use of the compiler
//
// when working with these traits, either to keep type parameters or make them
// into trait objects is mostly a matter of taste. (at least when most traits
// can be made into trait objects.) there are certain cases where a type
// parameter must be inroduced e.g. when it is propagated across interfaces, and
// ther are also other cases where a trait object must be used e.g. when sending
// events in circle, and working with type-erasured events (so the consumer type
// appears everywhere instead of the event types), trait objects must be used
// somewhere to break infinite type recursion. other than these there's no
// preference on which style to go with
//
// there's no pursuit on project-level code organization for software
// engineering or whatever reason. ideally there suppose to be one big `src/`
// that every single line of code lives in. (until it spends whole night to
// finish compiling.)
//
// the reason to have a separate `tools` is because those code are for distinct
// purpose. the main part of this codebase is distributed code, that should be
// packed into single executable, rsync to all over the world, and keep running
// until crashed. however the code in `tools` are for orchestrating evaluations.
// they keep on local machine and not go anywhere. they self-terminates. they
// behave as clients instead of servers, or just as simple scripts. there's no
// dependency from `tools/` to `src/`. there's no dependency from `src/` to
// `tools/` except some shared serialization schemas. the interfaces between
// them are built on HTTP. i believe this design results in good separation of
// conern
//
// for `crates` the story is much simpler. `src/` should be compilable against
// plain Ubuntu system (I use Debian though). and code live in `crates` if they
// have further system requirements. currently there's `crates/entropy` that
// requires cmake, llvm-dev, libclang-dev and clang to be present. there may be
// another crate for dpdk stuff later
//
// well, not particular useful note, but this codebase can make so much use of
// trait alias. if some day i surprisingly switch to nightly toolchain, this
// must be a huge reason for it
//
// previously there's essential difference between `SendEvent` and
// `SendMessage`: the `Addr` was an associated type. after i realize that the
// address may not be unique for certain implementations, `SendMessage<A, M>`
// is almost equivalent as `SendEvent<(A, M)>`. this is exactly what certain
// nets e.g. `PeerNet` are doing
//
// i choose to not build universal connection between them because they are
// expected to be used in different context: `SendEvent` probably sends to local
// receipant i.e. `impl OnEvent`, thus it's possible to work with type-erased
// events; `SendMessage` on contrast sends to "the outside world", so the
// sending cannot be directly hooked up with the consuming and type erasure
// cannot be performed. if multiple `M`s has been `impl SendMessage<A, M>`, then
// they are probably get unified into single representation as the shared "wire
// format" e.g. enum variants.
//
// i feel sorry to have so many `anyhow::Result<()>` all over the codebase. i
// should have aliased it so i foreseed it's this many. nevertheless, i guess
// the enhancement of std error will not get stablized in any time soon, so
// there still a long time before i need to consider replace these results with
// `Result<(), Box<dyn Error + Send + Sync>>`. probably the codebase has been
// gone before that
//
// the error rules are roughly:
//   use panic when it suppose to not panic. it's kind of in the same situation
//   of unsafe in this codebase: i know and guarantee it is safe/not going to
//   panic, but cannot convince compiler because of the lack of expressiveness,
//   so i assert it to compiler. one decent example is to "pop" a random entry
//   from hash map
//     if let Some(k) = m.keys().next() { let v = m.remove(k).unwrap(); }
//   another is to turn non-zero integer literal into NonZero* type
//
//   use unstructured/ad-hoc error i.e. anyhow::anyhow!() when there's no upper
//   layer can handle that error, and it suppose to lead to crash. the
//   oppotunity of making such conclusion is usually rare, but in this codebase
//   i happen to get a lot of chance, since myself is the only downstream user
//   of the library part of code. actually, all errors has been unstructured
//   until now, and the very first ones that are likely to be turned into
//   structured errors are the ones for disconnected channels. those are usually
//   because of unexpected exiting that caused by some other, root errors, so
//   filter them off helps focus on other meaningful errors
//
//   and lastly, create on-demand structured errors when it is required to be
//   downcast and captured. in some sense unstructured error offers more
//   information on control flow: it always leads to program termination, and
//   should never happen when evaluation goes as expect, so it is preferred over
//   structured error
//
// the boundary between panic and unstructured error becomes blur: they both
// should never happen when everything goes well, but also both are still
// possible to happen (for panic it is when i make mistakes), so the artifacts
// must be prepared for both to happen and try their best to do reliable
// crashing. this is the main reason that i used to only do panic in the past.
// the other reasons include panic has builtin backtrace support, which (
// denimished after anyhow supports backtrace in stable), and panic is reported
// when it's happening (no matter whether or not get caught later), so it's very
// convenient to reliably both crash and report by using panic and
// `panic = "abort"` in cargo config. The latter has disadvantage of panic
// messages mess up when there's multiple panic happens (why not locking
// standard error before reporting? guess for the case where execution panic
// while holding the standard error lock). also, `panic = "abort"` does not work
// well with test profile, so test cases behavior will diverge from expectation
//
// tentatively the rule of thumb is that, when things indeed go unexpectedly, if
// i will tend to rule out the unexpected case and stick on the original
// assumption, then go with panic (which is also aligned with the intent
// of assert!(); the most concise equivalent `if (...) { anyhow::bail!() }`` is
// still too verbose); otherwise, propagate the error. in another word, whoever
// causes the panic take responsibility of consolidating the "truth"; propagate
// if the truth cannot/should not be built around this layer
//
// it feels like no much difference to panic or return error from top level
// routine, so the choice is mostly credit to convenience: it's easier to return
// error from a regular `main` function, but for axum server handlers, panic
// is probably the easier one
//
// (well, if you want to propagate error from server to client, then return
// error is much more preferred; but in the discussed use case there's direct
// access to the server's internal log, and the only thing i need to know is
// which server looks abnormal, the demand panic can fulfill automatically with
// a 500 http code)
