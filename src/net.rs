// core network abstractions
//
// the central network trait `SendMessage<A, M>`, reads as sending a message M
// to address A. the model is rather oneshot: no concept of connection or
// persistent pipe is exposing to user. in this codebase this is a proper model
// for many p2p/fault tolerance protocols: even if the protocols have stronger
// access/control to the underlying details, they hardly benefit from that. That
// said, a connection-oriented interface can be added alongside
// `SendMessage<_, _>` if desired
//
// this fact causes a rather thick adapting layer when impl SendMessage<_, _>
// over network stack like TCP. there are many details user cannot specify, but
// they probably do not care anyway if they are happy with SendMessage<_, _>

use std::{fmt::Debug, hash::Hash, marker::PhantomData};

use bincode::Options as _;
use bytes::Bytes;
use derive_where::derive_where;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::{
    event::{erased::OnEvent, BlackHole, SendEvent},
    workload::events::Invoke,
};

pub trait Addr:
    Send + Sync + Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + 'static
{
}
impl<T: Send + Sync + Clone + Eq + Hash + Debug + Serialize + DeserializeOwned + 'static> Addr
    for T
{
}

pub trait Buf: AsRef<[u8]> + Into<Vec<u8>> + Send + Sync + Clone + From<Vec<u8>> + 'static {}

// not blanket impl here because we further require `Buf` to be "cheaply" cloned
impl Buf for bytes::Bytes {}

const MAX_BUF_LEN: usize = 16 << 20; // hard limit of single serialized message

// terms about nets that used in this codebase
// raw net: implementation of `SendMessage<_, impl Buf>`
// message net: implement `SendMessage<_, M>` for some structural message type
// `M`, potentially through wrapping some raw net and adding serialization upon
// it (and such implementation usually becomes a type alias of `MessageNet`
// below)
// socket net: implementation of `SendMessage<SocketAddr, _>`
// the implementation that wraps a socket net and translates other address types
// into `SocketAddr` can be called as routing net, i guess

// the `A` parameter used to be an associated type, and later get lifted to
// contravariant position to enable implementations to send to multiple address
// types
// there used to be a dedicated trait for raw net, but later get merged into
// this universal trait
// these result in the fact that `Addr` and `Buf` traits are not directly
// mentioned in sending trait anymore. nevertheless, constraint type parameters
// with them manually when necessary

// it's obvious that `M` is not required to `impl Buf` for all cases, but `A`
// is neither required to `impl Addr` for all cases
// only the "public" `A`s, e.g. the ones that are sent as part of network
// messages, are required to `impl Addr`, because e.g. network messages demand
// it
// perhaps `Addr` is not the best name for the trait
pub trait SendMessage<A, M> {
    // an alternative choice is to accept `message: &M` to avoid overhead of moving large messages,
    // since structural messages needs to be serialized before sent, which usually can be done
    // against borrowed messages
    // the reasons against that choice is that, firstly this `SendMessage` is a general propose
    // network abstraction, covering the implementations that does not serialize messages e.g.
    // sending through in memory channels, thus demands message ownership
    // secondly, even only considering serializing implementations, the implementation may serialize
    // the message with certain extra fields e.g. tagging the type `M`. although these fields can be
    // filled into byte buffer, we would prefer to instead performing transformation on the original
    // message before serialization to do the trick, to e.g. enjoy type safety. certain
    // transformation e.g. `Into` is easy to use and requires owned messages, so the trait follows
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()>;
}

// is it good practice to reuse?
impl<A, M> SendMessage<A, M> for BlackHole {
    fn send(&mut self, _: A, _: M) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<T: ?Sized + SendMessage<A, M>, A, M> SendMessage<A, M> for Box<T> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        T::send(self, dest, message)
    }
}

// an `IterAddr` type for broadcast (or multicast, depends on context)
// service provider site should impl `SendMessage<IterAddr<_>, _>`, to fully
// leverage the combinator that works with generic address types

// this is not usable without "real" higher ranked trait bound i.e.
// T: for<I: Iterator<Item = AddrType>> SendMessage<IterAddr<I>, MessageType>
// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
// pub struct IterAddr<I>(pub I);

// this is usable enough, just still feels not very decent, and performance will
// be limited, certainly
pub struct IterAddr<'a, A>(pub &'a mut (dyn Iterator<Item = A> + Send + Sync));
// it may seem like a default implementation of SendMessage<IterAddr<A>, M>
// should be provided as long as SendMessage<A, M> and M is Clone. but in this
// codebase "address" has been abused and certain address iterator e.g.
// IterAddr<All> does not really make sense

// the user site interface. avoid writing out `IterAddr` explicitly
// TODO better name
pub trait SendMessageToEach<A, M>: for<'a> SendMessage<IterAddr<'a, A>, M> {}
impl<T: for<'a> SendMessage<IterAddr<'a, A>, M>, A, M> SendMessageToEach<A, M> for T {}
// this has to go into a separated trait because the method is not object safe,
// while `SendMessageToEach` is a popular candidate for trait object
pub trait SendMessageToEachExt<A, M>: SendMessageToEach<A, M> {
    fn send_to_each(
        &mut self,
        mut addrs: impl Iterator<Item = A> + Send + Sync,
        message: M,
    ) -> anyhow::Result<()> {
        SendMessage::send(self, IterAddr(&mut addrs), message)
    }
}
impl<T: ?Sized + SendMessageToEach<A, M>, A, M> SendMessageToEachExt<A, M> for T {}

pub mod events {
    #[derive(Debug, Clone)]
    pub struct Recv<M>(pub M);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SendAddr<E>(pub E);

#[derive_where(Debug)]
pub struct Auto<A>(PhantomData<A>); // TODO better name

impl<E: SendEvent<M>, M> SendMessage<SendAddr<E>, M> for Auto<SendAddr<E>> {
    fn send(&mut self, mut dest: SendAddr<E>, message: M) -> anyhow::Result<()> {
        dest.0.send(message)
    }
}

#[derive_where(Debug, Clone; T)]
pub struct MessageNet<T, M>(T, PhantomData<M>);

impl<T, M> MessageNet<T, M> {
    pub fn new(raw_net: T) -> Self {
        Self(raw_net, Default::default())
    }
}

impl<T, M> From<T> for MessageNet<T, M> {
    fn from(value: T) -> Self {
        Self::new(value)
    }
}

impl<T: SendMessage<A, Bytes>, A, M: Into<N>, N: Serialize> SendMessage<A, M> for MessageNet<T, N> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        // the `dest` may be an IterAddr, use Bytes to reduce cloning overhead
        let buf = Bytes::from(bincode::options().serialize(&message.into())?);
        self.0.send(dest, buf)
    }
}

pub fn deserialize<M: DeserializeOwned>(buf: &[u8]) -> anyhow::Result<M> {
    bincode::options()
        .allow_trailing_bytes()
        .deserialize(buf)
        .map_err(Into::into)
}

#[derive(Debug, Clone, derive_more::Deref, derive_more::DerefMut)]
pub struct IndexNet<N, A> {
    #[deref]
    #[deref_mut]
    inner_net: N,
    addrs: Vec<A>,
    local_index: Option<usize>,
}

impl<N, A> IndexNet<N, A> {
    pub fn new(inner_net: N, addrs: Vec<A>, local_index: impl Into<Option<usize>>) -> Self {
        Self {
            inner_net,
            addrs,
            local_index: local_index.into(),
        }
    }
}

impl<N: SendMessage<A, M>, A: Addr, M, B: Into<usize>> SendMessage<B, M> for IndexNet<N, A> {
    fn send(&mut self, dest: B, message: M) -> anyhow::Result<()> {
        let dest = self
            .addrs
            .get(dest.into())
            .ok_or(anyhow::format_err!("index out of bound"))?;
        self.inner_net.send(dest.clone(), message)
    }
}

// intentionally not impl `Addr`; this must be consumed before reaching raw nets
#[derive(Debug)]
pub struct All;

impl<N: for<'a> SendMessageToEach<A, M>, A: Addr, M> SendMessage<All, M> for IndexNet<N, A> {
    fn send(&mut self, All: All, message: M) -> anyhow::Result<()> {
        let addrs = self.addrs.iter().enumerate().filter_map(|(id, addr)| {
            if self.local_index == Some(id) {
                None
            } else {
                Some(addr.clone())
            }
        });
        self.inner_net.send_to_each(addrs, message)
    }
}

#[derive(Debug)]
pub struct InvokeNet<C>(pub C);

// currently InvokeNet ignore the address argument, who will receive the message
// is specified by the invocation consumer
// for example, if the invocation is consumed by a replication client, then
// every replica will receive the message, even the client can do little on this
impl<C: SendEvent<Invoke<Payload>>, A, B: Buf> SendMessage<A, B> for InvokeNet<C> {
    fn send(&mut self, _: A, message: B) -> anyhow::Result<()> {
        self.0.send(Invoke(Payload(message.into())))
    }
}

#[derive(Debug, Clone)]
pub struct Detach<E>(pub E);

pub struct DetachSend<A, M>(pub A, pub M);

impl<E: SendEvent<DetachSend<A, M>>, A, M> SendMessage<A, M> for Detach<E> {
    fn send(&mut self, dest: A, message: M) -> anyhow::Result<()> {
        self.0.send(DetachSend(dest, message))
    }
}

// probably not useful
impl<N: SendMessage<A, M>, A, M> OnEvent<DetachSend<A, M>> for N {
    fn on_event(
        &mut self,
        DetachSend(dest, message): DetachSend<A, M>,
        _: &mut impl crate::event::Timer,
    ) -> anyhow::Result<()> {
        SendMessage::send(self, dest, message)
    }
}

#[derive(
    Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default, derive_more::Deref, Serialize, Deserialize,
)]
pub struct Payload(pub Vec<u8>);

impl Debug for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Ok(s) = std::str::from_utf8(&self.0) {
            write!(f, "Payload(\"{s}\")")
        } else {
            write!(
                f,
                "Payload({}{})",
                self.0
                    .iter()
                    .map(|b| format!("{b:02x}"))
                    .take(32)
                    .collect::<Vec<_>>()
                    .concat(),
                if self.0.len() > 32 {
                    format!(".. <len {}>", self.0.len())
                } else {
                    String::new()
                }
            )
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Request<A> {
    pub client_id: u32,
    pub client_addr: A,
    pub seq: u32,
    pub op: Payload,
}

// cSpell:words bincode deque

pub mod blocking;
pub mod dispatch;
pub mod kademlia;
pub mod session;

pub use dispatch::Dispatch;

// cSpell:words oneshot kademlia bincode
