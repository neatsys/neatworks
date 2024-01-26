pub mod app;
pub mod blob;
pub mod crypto;
pub mod event;
pub mod kademlia;
pub mod net;
pub mod pbft;
pub mod replication;
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
// into trait objects is mostly by personal tastes. (at least when most traits
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
