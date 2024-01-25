pub mod app;
pub mod crypto;
pub mod event;
pub mod kademlia;
pub mod net;
pub mod pbft;
pub mod replication;
pub mod unreplicated;
pub mod worker;

// develop notes that does not apply to any specific code
// (start writing dev docs usually follows by a complete rewriting, hope not
// this time)
//
// there has been a lot of generic around this codebase, naming convention
// is roughly: `M`/`N` for messages/events, `S`/`U` for state machine (that
// ususally `impl OnEvent`) (and `T` is not confused with representing general
// types), `N`/`M` for nets, `E` for `impl SendEvent` (i.e. emitter), `A`/`B`
// for addresses
// (yeah, i also just realized the awful reusing of `M`/`N`)
//
// although does not look like so, but the spirit is to avoid generic/trait
// usage as much as possible. always prefer enum polymophism than trait one.
// existing enum polymophism includes crypto, worker, and app (which is trait
// for now, but probably will be converted soon or later). the user of this
// codebase is generally assumed to keep this codebase at a writable place (i.e.
// not as a git/crates.io dependency), so more enum variants can always be added
// if necessary. every trait comes with a reason
//
// the reason for `SendEvent` and `SendMessage` is that the user/implementor
// may require/support sending various number of event/message types (and for
// `SendMessage`, the number of supported address types may also vary). the
// method interface is not aligned, since some implementation variants may
// support to send certain kind of event/message and others may not.
// the enum based approach would require a event enum and a sender enum, then
// simultaneously match on them and only forward the sensible combinations,
// leave the rest ones with `unimplemented!()`. that would be terrible to
// maintain, and what's worse, certain implementor has type parameters on
// its own, and can support different kind of events base on what parameters
// are given to itself. the enum must take all these type parameters, makes the
// approach infeasible.
//
// the reason for `Addr` is that most components only work with one address type
// (plus some auxiliry types like `AllReplica`), so an address enum will
// introduce more runtime check and more places that bugs may sneak in. it is
// anonying to keep a generic address type everywhere, but i have to choose
// either this or anonying downcasts every time the address is evaluated. i
// decide to choose the type parameter since it make better use of the compiler
//
// there's no pursuit on code organization for software engineering or whatever
// reason. ideally there suppose to be one big `src/` that every single line of
// code lives in. (until it spends whole night to finish compiling.)
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
