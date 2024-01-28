use std::{collections::HashMap, fmt::Debug, num::NonZeroUsize, sync::Arc};

use augustus::{
    crypto::DigestHash,
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    net::{Addr, SendMessage},
    worker::erased::Worker,
};
use serde::{Deserialize, Serialize};
use wirehair::Encoder;

use crate::{Encode, GetOk, NewEncoder, Put, PutOk};

// well, technically client and peer can run on different address types and we
// can have SendMessage<PeerType, Put<ClientType>>
// but come on, even generalize the SocketAddr away at first is so
// overenginnering :)
pub trait PutNet<A>: SendMessage<A, Put<A>> {}
impl<T: SendMessage<A, Put<A>>, A> PutNet<A> for T {}

// this becomes an abusing reuse of `NewEncoder` and `Encode`
// thankfully the use of these events are quite local and there's only six days
// left until ddl
pub trait SendPutCodecEvent: SendEvent<NewEncoder> + SendEvent<Encode> {}
impl<T: SendEvent<NewEncoder> + SendEvent<Encode>> SendPutCodecEvent for T {}

pub struct PutClient<A> {
    chunk_len: u32,
    k: NonZeroUsize,
    addr: A,
    on_put: Option<OnPut>,
    peer_addrs: HashMap<u32, A>,
    net: Box<dyn PutNet<A> + Send + Sync>,
    codec_worker: CodecWorker,
}

pub type OnPut = Box<dyn FnOnce() -> anyhow::Result<()> + Send + Sync>;
pub type CodecWorker = Worker<(), dyn SendPutCodecEvent + Send + Sync>;

impl<A> Debug for PutClient<A> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutClient").finish_non_exhaustive()
    }
}

impl<A> PutClient<A> {
    pub fn new(
        chunk_len: u32,
        k: NonZeroUsize,
        addr: A,
        net: impl PutNet<A> + Send + Sync + 'static,
        codec_worker: CodecWorker,
    ) -> Self {
        Self {
            chunk_len,
            k,
            addr,
            net: Box::new(net),
            codec_worker,
            on_put: None,
            peer_addrs: Default::default(),
        }
    }

    pub fn operate(
        &mut self,
        buf: Vec<u8>,
        peer_addrs: Vec<A>,
        on_put: OnPut,
    ) -> anyhow::Result<()> {
        let replaced = self.on_put.replace(on_put);
        if replaced.is_some() {
            anyhow::bail!("operate while another operation in progress")
        }
        if buf.len() != self.chunk_len as usize * self.k.get() {
            anyhow::bail!(
                "expect buf with {} * {} bytes, actual {}",
                self.chunk_len,
                self.k,
                buf.len()
            )
        }
        assert!(self.peer_addrs.is_empty());
        for addr in peer_addrs {
            let mut index;
            while {
                index = rand::random();
                self.peer_addrs.contains_key(&index)
            } {}
            self.peer_addrs.insert(index, addr);
        }
        let chunk_len = self.chunk_len;
        self.codec_worker.submit(Box::new(move |(), sender| {
            sender.send(NewEncoder(
                Default::default(),
                Encoder::new(&buf, chunk_len)?,
            ))
        }))
    }
}

impl<A> OnEvent<NewEncoder> for PutClient<A> {
    fn on_event(
        &mut self,
        NewEncoder(_, encoder): NewEncoder,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let encoder = Arc::new(encoder);
        for index in self.peer_addrs.keys().copied() {
            let encoder = encoder.clone();
            self.codec_worker.submit(Box::new(move |(), sender| {
                let chunk = encoder.encode(index)?;
                sender.send(Encode(chunk.sha256(), index, chunk))
            }))?
        }
        Ok(())
    }
}

impl<A: Addr> OnEvent<Encode> for PutClient<A> {
    fn on_event(
        &mut self,
        Encode(chunk, index, buf): Encode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let put = Put(self.addr.clone(), chunk, buf);
        self.net.send(self.peer_addrs[&index].clone(), put)
    }
}

// it should be more decent to have separate message nets and on buf handlers
// for put and get clients, but i hate to split client net of peer into put net
// and get net
// it always has too many arguments
#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum Message {
    PutOk(PutOk),
    GetOk(GetOk),
}

pub type MessageNet<T> = augustus::net::MessageNet<T, Message>;
