use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    sync::Arc,
};

use augustus::{
    blob::{self, RecvBlob, Serve, Transfer},
    crypto::{Verifiable, H256},
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    kademlia::{self, FindPeer, FindPeerOk, PeerId},
    net::{deserialize, events::Recv, kademlia::Multicast, Addr, SendMessage},
    worker::erased::Worker,
};

use serde::{Deserialize, Serialize};
use wirehair::{Decoder, Encoder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    chunk: [u8; 32],
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InviteOk {
    chunk: [u8; 32],
    index: u32,
    proof: (),
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendFragment {
    chunk: [u8; 32],
    index: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pull {
    chunk: [u8; 32],
    peer_id: PeerId,
}

pub trait Net:
    SendMessage<Multicast, Invite> + SendMessage<PeerId, InviteOk> + SendMessage<Multicast, Pull>
{
}
impl<
        T: SendMessage<Multicast, Invite>
            + SendMessage<PeerId, InviteOk>
            + SendMessage<Multicast, Pull>,
    > Net for T
{
}

pub trait TransferBlob: SendEvent<Transfer<PeerId, SendFragment>> {}
impl<T: SendEvent<Transfer<PeerId, SendFragment>>> TransferBlob for T {}

#[derive(Debug, Clone)]
pub struct Put(pub [u8; 32], pub Vec<u8>);
#[derive(Debug, Clone)]
pub struct Get(pub [u8; 32]);

#[derive(Debug, Clone)]
pub struct PutOk(pub [u8; 32]);
#[derive(Debug, Clone)]
pub struct GetOk(pub [u8; 32], pub Vec<u8>);

pub trait Upcall: SendEvent<PutOk> + SendEvent<GetOk> {}
impl<T: SendEvent<PutOk> + SendEvent<GetOk>> Upcall for T {}

#[derive(Debug)]
pub struct NewEncoder([u8; 32], Encoder);
#[derive(Debug, Clone)]
pub struct Encode([u8; 32], u32, Vec<u8>);
#[derive(Debug)]
pub struct Decode([u8; 32], Decoder);
#[derive(Debug, Clone)]
pub struct Recover([u8; 32], Vec<u8>);

pub trait SendCodecEvent:
    SendEvent<NewEncoder> + SendEvent<Encode> + SendEvent<Decode> + SendEvent<Recover>
{
}
impl<T: SendEvent<NewEncoder> + SendEvent<Encode> + SendEvent<Decode> + SendEvent<Recover>>
    SendCodecEvent for T
{
}

pub trait SendFsEvent: SendEvent<fs::Store> + SendEvent<fs::Load> {}
impl<T: SendEvent<fs::Store> + SendEvent<fs::Load>> SendFsEvent for T {}

pub struct Peer {
    id: PeerId,
    fragment_len: u32,
    chunk_k: usize,
    chunk_n: usize,

    uploads: HashMap<[u8; 32], UploadState>,
    downloads: HashMap<[u8; 32], DownloadState>,
    persists: HashMap<[u8; 32], PersistState>,
    pending_pulls: HashMap<[u8; 32], Vec<PeerId>>,

    net: Box<dyn Net + Send + Sync>,
    blob: Box<dyn TransferBlob + Send + Sync>,
    upcall: Box<dyn Upcall + Send + Sync>,
    codec_worker: CodecWorker,
    fs: Box<dyn SendFsEvent + Send + Sync>,
}

pub type CodecWorker = Worker<(), dyn SendCodecEvent + Send + Sync>;

#[derive(Debug)]
struct UploadState {
    encoder: Arc<Encoder>,
    pending: HashMap<u32, PeerId>,
}

#[derive(Debug)]
struct DownloadState {
    recover: RecoverState,
}

#[derive(Debug)]
struct RecoverState {
    decoder: Option<Decoder>,
    pending: HashMap<u32, Vec<u8>>,
    received: HashSet<u32>,
}

impl RecoverState {
    fn new(fragment_len: u32, chunk_k: usize) -> anyhow::Result<Self> {
        Ok(Self {
            decoder: Some(Decoder::new(
                fragment_len as u64 * chunk_k as u64,
                fragment_len,
            )?),
            pending: Default::default(),
            received: Default::default(),
        })
    }
}

#[derive(Debug)]
struct PersistState {
    index: u32,
    status: PersistStatus,
}

#[derive(Debug)]
enum PersistStatus {
    Recovering(RecoverState),
    Storing,
    Available,
}

impl Debug for Peer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer").finish_non_exhaustive()
    }
}

impl OnEvent<Put> for Peer {
    fn on_event(&mut self, Put(chunk, buf): Put, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        if buf.len() != self.fragment_len as usize * self.chunk_k {
            anyhow::bail!(
                "expect chunk len {} * {}, actual {}",
                self.fragment_len,
                self.chunk_k,
                buf.len()
            )
        }
        let fragment_len = self.fragment_len;
        self.codec_worker.submit(Box::new(move |(), sender| {
            let encoder = Encoder::new(&buf, fragment_len)?;
            sender.send(NewEncoder(chunk, encoder))
        }))
    }
}

impl OnEvent<NewEncoder> for Peer {
    fn on_event(
        &mut self,
        NewEncoder(chunk, encoder): NewEncoder,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let replaced = self.uploads.insert(
            chunk,
            UploadState {
                encoder: encoder.into(),
                pending: Default::default(),
            },
        );
        if replaced.is_some() {
            anyhow::bail!("duplicated upload chunk {}", H256(chunk))
        }
        let invite = Invite {
            chunk,
            peer_id: self.id,
        };
        self.net.send(Multicast(chunk, self.chunk_n as _), invite)
    }
}

impl OnEvent<Recv<Invite>> for Peer {
    fn on_event(
        &mut self,
        Recv(invite): Recv<Invite>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // technically this is fine, just for more stable evaluation
        // the PUT/GET peers are expected to perform additional object-level codec
        // exclude them from chunk-level workload prevents performance interference
        if invite.peer_id == self.id {
            return Ok(());
        }
        let index = 0; // TODO
        self.persists.entry(invite.chunk).or_insert(PersistState {
            index,
            status: PersistStatus::Recovering(RecoverState::new(self.fragment_len, self.chunk_k)?),
        });
        // TODO provide a way to supress unnecessary following `SendFragment`
        let invite_ok = InviteOk {
            chunk: invite.chunk,
            index,
            proof: (),
            peer_id: self.id,
        };
        self.net.send(invite.peer_id, invite_ok)
    }
}

impl OnEvent<Recv<InviteOk>> for Peer {
    fn on_event(
        &mut self,
        Recv(invite_ok): Recv<InviteOk>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.uploads.get_mut(&invite_ok.chunk) else {
            return Ok(());
        };
        state.pending.insert(invite_ok.index, invite_ok.peer_id);
        let encoder = state.encoder.clone();
        self.codec_worker.submit(Box::new(move |(), sender| {
            let fragment = encoder.encode(invite_ok.index)?;
            sender.send(Encode(invite_ok.chunk, invite_ok.index, fragment))
        }))
    }
}

impl OnEvent<Encode> for Peer {
    fn on_event(
        &mut self,
        Encode(chunk, index, fragment): Encode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.uploads.get(&chunk) else {
            return Ok(());
        };
        let Some(peer_id) = state.pending.get(&index) else {
            // is this ok?
            return Ok(());
        };
        let send_fragment = SendFragment { chunk, index };
        self.blob.send(Transfer(*peer_id, send_fragment, fragment))
    }
}

impl OnEvent<RecvBlob<SendFragment>> for Peer {
    fn on_event(
        &mut self,
        RecvBlob(send_fragment, fragment): RecvBlob<SendFragment>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(state) = self.downloads.get_mut(&send_fragment.chunk) {
            return state.recover.submit_decode(
                send_fragment.chunk,
                send_fragment.index,
                fragment,
                &self.codec_worker,
            );
        }
        if let Some(state) = self.persists.get_mut(&send_fragment.chunk) {
            let PersistStatus::Recovering(recover) = &mut state.status else {
                return Ok(());
            };
            if send_fragment.index == state.index {
                state.status = PersistStatus::Storing;
                return self
                    .fs
                    .send(fs::Store(send_fragment.chunk, state.index, fragment));
            }
            return recover.submit_decode(
                send_fragment.chunk,
                send_fragment.index,
                fragment,
                &self.codec_worker,
            );
        }
        Ok(())
    }
}

impl RecoverState {
    fn submit_decode(
        &mut self,
        chunk: [u8; 32],
        index: u32,
        fragment: Vec<u8>,
        worker: &CodecWorker,
    ) -> Result<(), anyhow::Error> {
        if !self.received.insert(index) {
            return Ok(());
        }
        if let Some(mut decoder) = self.decoder.take() {
            worker.submit(Box::new(move |(), sender| {
                if decoder.decode(index, &fragment)? {
                    sender.send(Decode(chunk, decoder))
                } else {
                    sender.send(Recover(chunk, decoder.recover()?))
                }
            }))
        } else {
            self.pending.insert(index, fragment);
            Ok(())
        }
    }
}

impl OnEvent<fs::StoreOk> for Peer {
    fn on_event(
        &mut self,
        fs::StoreOk(chunk): fs::StoreOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.persists.get_mut(&chunk) else {
            // is this ok?
            return Ok(());
        };
        state.status = PersistStatus::Available;
        Ok(()) // TODO reply
    }
}

impl OnEvent<Get> for Peer {
    fn on_event(&mut self, Get(chunk): Get, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        let replaced = self.downloads.insert(
            chunk,
            DownloadState {
                recover: RecoverState::new(self.fragment_len, self.chunk_k)?,
            },
        );
        if replaced.is_some() {
            anyhow::bail!("duplicated download chunk {}", H256(chunk))
        }
        let pull = Pull {
            chunk,
            peer_id: self.id,
        };
        self.net.send(Multicast(chunk, self.chunk_n as _), pull)
    }
}

impl OnEvent<Recv<Pull>> for Peer {
    fn on_event(&mut self, Recv(pull): Recv<Pull>, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        let Some(state) = self.persists.get(&pull.chunk) else {
            return Ok(());
        };
        if !matches!(state.status, PersistStatus::Available) {
            return Ok(());
        }
        self.pending_pulls
            .entry(pull.chunk)
            .or_default()
            .push(pull.peer_id);
        self.fs.send(fs::Load(pull.chunk, state.index, true))
    }
}

impl OnEvent<fs::LoadOk> for Peer {
    fn on_event(
        &mut self,
        fs::LoadOk(chunk, index, fragment): fs::LoadOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(pending) = self.pending_pulls.remove(&chunk) else {
            return Ok(());
        };
        let send_fragment = SendFragment { chunk, index };
        for peer_id in pending {
            // cloning overhead can be mitigate with `Bytes`, but `Transfer` then need to accept
            // any `impl Buf` which seems nontrivial
            // the number of pending peers will be 1 anyway
            self.blob
                .send(Transfer(peer_id, send_fragment.clone(), fragment.clone()))?
        }
        Ok(())
    }
}

impl OnEvent<Decode> for Peer {
    fn on_event(
        &mut self,
        Decode(chunk, decoder): Decode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(state) = self.downloads.get_mut(&chunk) {
            state.recover.on_decode(chunk, decoder, &self.codec_worker)
        } else {
            Ok(())
        }
    }
}

impl RecoverState {
    fn on_decode(
        &mut self,
        chunk: [u8; 32],
        decoder: Decoder,
        worker: &CodecWorker,
    ) -> anyhow::Result<()> {
        assert!(self.decoder.is_none());
        if let Some(&index) = self.pending.keys().next() {
            let fragment = self.pending.remove(&index).unwrap();
            self.submit_decode(chunk, index, fragment, worker)?
        } else {
            self.decoder = Some(decoder)
        }
        Ok(())
    }
}

impl OnEvent<Recover> for Peer {
    fn on_event(
        &mut self,
        Recover(chunk, buf): Recover,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if self.downloads.remove(&chunk).is_some() {
            self.upcall.send(GetOk(chunk, buf))
        } else {
            todo!()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum Message<A> {
    Invite(Invite),
    InviteOk(InviteOk),
    Pull(Pull),

    FindPeer(Verifiable<FindPeer<A>>),
    FindPeerOk(Verifiable<FindPeerOk<A>>),

    BlobServe(Serve<SendFragment>),
}

pub type MessageNet<T, A> = augustus::net::MessageNet<T, Message<A>>;

pub trait SendRecvEvent:
    SendEvent<Recv<Invite>> + SendEvent<Recv<InviteOk>> + SendEvent<Recv<Pull>>
{
}
impl<T: SendEvent<Recv<Invite>> + SendEvent<Recv<InviteOk>> + SendEvent<Recv<Pull>>> SendRecvEvent
    for T
{
}

pub fn on_buf<A: Addr>(
    buf: &[u8],
    entropy_sender: &mut impl SendRecvEvent,
    kademlia_sender: &mut impl kademlia::SendRecvEvent<A>,
    blob_sender: &mut impl blob::SendRecvEvent<SendFragment>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        Message::Invite(message) => entropy_sender.send(Recv(message)),
        Message::InviteOk(message) => entropy_sender.send(Recv(message)),
        Message::Pull(message) => entropy_sender.send(Recv(message)),
        Message::FindPeer(message) => kademlia_sender.send(Recv(message)),
        Message::FindPeerOk(message) => kademlia_sender.send(Recv(message)),
        Message::BlobServe(message) => blob_sender.send(Recv(message)),
    }
}

pub mod fs {
    use std::{fmt::Debug, path::Path};

    use augustus::{crypto::H256, event::SendEvent};
    use tokio::{
        fs::{create_dir, read, remove_dir_all, write},
        sync::mpsc::UnboundedReceiver,
        task::JoinSet,
    };

    #[derive(Clone)]
    pub struct Store(pub [u8; 32], pub u32, pub Vec<u8>);

    impl Debug for Store {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Store")
                .field("chunk", &H256(self.0))
                .field("index", &self.1)
                .field("data", &format!("<{} bytes>", self.2.len()))
                .finish()
        }
    }

    #[derive(Debug, Clone)]
    // Load(chunk, index, true) will delete fragment file while loading
    // not particual useful in practice, but good for evaluation with bounded storage usage
    pub struct Load(pub [u8; 32], pub u32, pub bool);

    #[derive(Debug, Clone)]
    pub struct StoreOk(pub [u8; 32]);

    #[derive(Clone)]
    pub struct LoadOk(pub [u8; 32], pub u32, pub Vec<u8>);

    impl Debug for LoadOk {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LoadOk")
                .field("chunk", &H256(self.0))
                .field("index", &self.1)
                .field("data", &format!("<{} bytes>", self.2.len()))
                .finish()
        }
    }

    #[derive(Debug, Clone, derive_more::From)]
    pub enum Event {
        Store(Store),
        Load(Load),
    }

    pub trait Upcall: SendEvent<StoreOk> + SendEvent<LoadOk> {}
    impl<T: SendEvent<StoreOk> + SendEvent<LoadOk>> Upcall for T {}

    pub async fn session(
        path: impl AsRef<Path>,
        mut events: UnboundedReceiver<Event>,
        mut upcall: impl Upcall,
    ) -> anyhow::Result<()> {
        let mut store_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut load_tasks = JoinSet::<anyhow::Result<_>>::new();
        loop {
            enum Select {
                Recv(Event),
                JoinNextStore([u8; 32]),
                JoinNextLoad(([u8; 32], u32, Vec<u8>)),
            }
            match tokio::select! {
                event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
                Some(result) = store_tasks.join_next() => Select::JoinNextStore(result??),
                Some(result) = load_tasks.join_next() => Select::JoinNextLoad(result??),
            } {
                Select::Recv(Event::Store(Store(chunk, index, fragment))) => {
                    let chunk_path = path.as_ref().join(format!("{:x}", H256(chunk)));
                    store_tasks.spawn(async move {
                        create_dir(&chunk_path).await?;
                        write(chunk_path.join(index.to_string()), fragment).await?;
                        Ok(chunk)
                    });
                }
                Select::Recv(Event::Load(Load(chunk, index, take))) => {
                    let chunk_path = path.as_ref().join(format!("{:x}", H256(chunk)));
                    load_tasks.spawn(async move {
                        let fragment = read(chunk_path.join(index.to_string())).await?;
                        if take {
                            remove_dir_all(chunk_path).await?
                        }
                        Ok((chunk, index, fragment))
                    });
                }
                Select::JoinNextStore(chunk) => upcall.send(StoreOk(chunk))?,
                Select::JoinNextLoad((chunk, index, fragment)) => {
                    upcall.send(LoadOk(chunk, index, fragment))?
                }
            }
        }
    }
}
