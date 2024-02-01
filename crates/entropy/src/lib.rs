use std::{
    collections::{HashMap, HashSet},
    fmt::Debug,
    mem::replace,
    num::NonZeroUsize,
    sync::Arc,
};

use augustus::{
    blob::{
        self,
        stream::{RecvBlob, Serve, Transfer},
    },
    crypto::{Crypto, PublicKey, Verifiable, H256},
    event::{
        erased::{OnEvent, Timer},
        SendEvent,
    },
    kademlia::{self, FindPeer, FindPeerOk, PeerId, Target},
    net::{deserialize, events::Recv, kademlia::Multicast, Addr, SendMessage},
    worker::erased::Worker,
};

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;
use wirehair::{Decoder, Encoder};

type Chunk = Target;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Invite {
    chunk: Chunk,
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InviteOk {
    chunk: Chunk,
    index: u32,
    proof: (),
    peer_id: PeerId,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendFragment {
    chunk: Chunk,
    index: u32,
    peer_id: Option<PeerId>, // Some(id) if expecting receipt
}

#[derive(Debug, Clone, Hash, Serialize, Deserialize)]
pub struct FragmentAvailable {
    chunk: Chunk,
    peer_id: PeerId,
    peer_key: PublicKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pull {
    chunk: Chunk,
    peer_id: PeerId,
}

// TODO generialize on address types, lifting up underlying `PeerNet` type
// parameter
pub trait Net:
    SendMessage<Multicast, Invite>
    + SendMessage<PeerId, InviteOk>
    + SendMessage<PeerId, Verifiable<FragmentAvailable>>
    + SendMessage<Multicast, Pull>
{
}
impl<
        T: SendMessage<Multicast, Invite>
            + SendMessage<PeerId, InviteOk>
            + SendMessage<PeerId, Verifiable<FragmentAvailable>>
            + SendMessage<Multicast, Pull>,
    > Net for T
{
}

pub trait TransferBlob: SendEvent<Transfer<PeerId, SendFragment>> {}
impl<T: SendEvent<Transfer<PeerId, SendFragment>>> TransferBlob for T {}

#[derive(Debug, Clone)]
pub struct Put<K>(pub K, pub Bytes);
#[derive(Debug, Clone)]
pub struct Get<K>(pub K);

#[derive(Debug, Clone)]
pub struct PutOk<K>(pub K);
#[derive(Debug, Clone)]
pub struct GetOk<K>(pub K, pub Vec<u8>);

pub trait Upcall<K>: SendEvent<PutOk<K>> + SendEvent<GetOk<K>> {}
impl<T: SendEvent<PutOk<K>> + SendEvent<GetOk<K>>, K> Upcall<K> for T {}

#[derive(Debug)]
pub struct NewEncoder(Chunk, Encoder);
#[derive(Debug, Clone)]
pub struct Encode(Chunk, u32, Vec<u8>);
#[derive(Debug)]
pub struct Decode(Chunk, Decoder);
#[derive(Debug, Clone)]
pub struct Recover(Chunk, Vec<u8>);
#[derive(Debug, Clone)]
pub struct RecoverEncode(Chunk, Vec<u8>);

pub trait SendCodecEvent:
    SendEvent<NewEncoder>
    + SendEvent<Encode>
    + SendEvent<Decode>
    + SendEvent<Recover>
    + SendEvent<RecoverEncode>
{
}
impl<
        T: SendEvent<NewEncoder>
            + SendEvent<Encode>
            + SendEvent<Decode>
            + SendEvent<Recover>
            + SendEvent<RecoverEncode>,
    > SendCodecEvent for T
{
}

#[derive(Debug, Clone)]
pub struct SendCodec<E>(pub E);

impl<'a, E: SendCodecEvent + Send + Sync + 'a> AsMut<dyn SendCodecEvent + Send + Sync + 'a>
    for SendCodec<E>
{
    fn as_mut(&mut self) -> &mut (dyn SendCodecEvent + Send + Sync + 'a) {
        &mut self.0
    }
}

pub trait SendFsEvent:
    SendEvent<fs::Store> + SendEvent<fs::Load> + SendEvent<fs::Download>
{
}
impl<T: SendEvent<fs::Store> + SendEvent<fs::Load> + SendEvent<fs::Download>> SendFsEvent for T {}

pub struct Peer<K> {
    id: PeerId,
    fragment_len: u32,
    chunk_k: NonZeroUsize, // the number of honest peers to recover a chunk
    // the number of peers that, with high probablity at least `k` peers of them are honest
    chunk_n: NonZeroUsize,
    chunk_m: NonZeroUsize, // the number of peers that Put peer send Invite to
    // `n` should be derived from `k` and the upper bound of faulty portion
    // `m` should be derived from `n` and the upper bound of faulty portion, because Put can only
    // be concluded when at least `n` peers replies FragmentAvailable, and the worst case is that
    // only honest peers do so
    // alternatively, Put peer can incrementally Invite more and more peers until enough peers
    // replies. that approach would involve timeout and does not fit evaluation purpose
    //
    uploads: HashMap<Chunk, UploadState<K>>,
    downloads: HashMap<Chunk, DownloadState<K>>,
    persists: HashMap<Chunk, PersistState>,
    pending_pulls: HashMap<Chunk, Vec<PeerId>>,

    net: Box<dyn Net + Send + Sync>,
    blob: Box<dyn TransferBlob + Send + Sync>,
    upcall: Box<dyn Upcall<K> + Send + Sync>,
    codec_worker: CodecWorker,
    fs: Box<dyn SendFsEvent + Send + Sync>,
    // well, in the context of entropy "computationally intensive" refers to some millisecond-scale
    // computational workload (which is what `codec_worker` for), and the system throughput is
    // tightly bounded on bandwidth (instead of pps) so saving stateful-processing overhead becomes
    // marginal for improving performance
    // in conclusion, do crypto inline, less event and less concurrency to consider :)
    crypto: Crypto<PeerId>,
}

pub type CodecWorker = Worker<(), dyn SendCodecEvent + Send + Sync>;

#[derive(Debug)]
struct UploadState<K> {
    preimage: K,
    encoder: Option<Arc<Encoder>>,
    pending: HashMap<u32, PeerId>,
    available: HashSet<PeerId>,
    cancel: CancellationToken,
}

#[derive(Debug)]
struct DownloadState<K> {
    preimage: K,
    recover: RecoverState,
}

#[derive(Debug)]
struct RecoverState {
    decoder: Option<Decoder>,
    pending: HashMap<u32, Vec<u8>>,
    received: HashSet<u32>,
    cancel: CancellationToken,
}

impl RecoverState {
    fn new(fragment_len: u32, chunk_k: NonZeroUsize) -> anyhow::Result<Self> {
        Ok(Self {
            decoder: Some(Decoder::new(
                fragment_len as u64 * chunk_k.get() as u64,
                fragment_len,
            )?),
            pending: Default::default(),
            received: Default::default(),
            cancel: CancellationToken::new(),
        })
    }
}

#[derive(Debug)]
struct PersistState {
    index: u32,
    status: PersistStatus,
    notify: Option<PeerId>,
}

#[derive(Debug)]
enum PersistStatus {
    Recovering(RecoverState),
    Storing,
    Available,
}

impl<K> Debug for Peer<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Peer").finish_non_exhaustive()
    }
}

impl<K> Peer<K> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: PeerId,
        crypto: Crypto<PeerId>,
        fragment_len: u32,
        chunk_k: NonZeroUsize,
        chunk_n: NonZeroUsize,
        chunk_m: NonZeroUsize,
        net: impl Net + Send + Sync + 'static,
        blob: impl TransferBlob + Send + Sync + 'static,
        upcall: impl Upcall<K> + Send + Sync + 'static,
        codec_worker: CodecWorker,
        fs: impl SendFsEvent + Send + Sync + 'static,
    ) -> Self {
        Self {
            id,
            crypto,
            fragment_len,
            chunk_k,
            chunk_n,
            chunk_m,
            net: Box::new(net),
            blob: Box::new(blob),
            upcall: Box::new(upcall),
            codec_worker,
            fs: Box::new(fs),
            uploads: Default::default(),
            downloads: Default::default(),
            persists: Default::default(),
            pending_pulls: Default::default(),
        }
    }
}

pub trait Preimage {
    fn target(&self) -> Target;
}

impl Preimage for [u8; 32] {
    fn target(&self) -> Target {
        *self
    }
}

impl<K: Preimage> OnEvent<Put<K>> for Peer<K> {
    fn on_event(
        &mut self,
        Put(preimage, buf): Put<K>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if buf.len() != self.fragment_len as usize * self.chunk_k.get() {
            anyhow::bail!(
                "expect chunk len {} * {}, actual {}",
                self.fragment_len,
                self.chunk_k,
                buf.len()
            )
        }
        let chunk = preimage.target();
        let replaced = self.uploads.insert(
            chunk,
            UploadState {
                preimage,
                encoder: None,
                pending: Default::default(),
                available: Default::default(),
                cancel: CancellationToken::new(),
            },
        );
        if replaced.is_some() {
            anyhow::bail!("duplicated upload chunk {}", H256(chunk))
        }
        let fragment_len = self.fragment_len;
        self.codec_worker.submit(Box::new(move |(), sender| {
            let encoder = Encoder::new(buf.into(), fragment_len)?;
            sender.send(NewEncoder(chunk, encoder))
        }))
    }
}

impl<K> OnEvent<NewEncoder> for Peer<K> {
    fn on_event(
        &mut self,
        NewEncoder(chunk, encoder): NewEncoder,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.uploads.get_mut(&chunk) else {
            return Ok(()); // ok?
        };
        let replaced = state.encoder.replace(encoder.into());
        assert!(replaced.is_none());
        let invite = Invite {
            chunk,
            peer_id: self.id,
        };
        // println!("multicast Invite {}", H256(chunk));
        self.net.send(Multicast(chunk, self.chunk_m), invite)
    }
}

impl<K> OnEvent<Recv<Invite>> for Peer<K> {
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
        let index = rand::random(); // TODO
        self.persists.entry(invite.chunk).or_insert(PersistState {
            index,
            status: PersistStatus::Recovering(RecoverState::new(self.fragment_len, self.chunk_k)?),
            notify: None,
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

impl<K> OnEvent<Recv<InviteOk>> for Peer<K> {
    fn on_event(
        &mut self,
        Recv(invite_ok): Recv<InviteOk>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // TODO verify
        if let Some(state) = self.uploads.get_mut(&invite_ok.chunk) {
            state.pending.insert(invite_ok.index, invite_ok.peer_id);
            let encoder = state.encoder.clone().unwrap();
            return self.codec_worker.submit(Box::new(move |(), sender| {
                let fragment = encoder.encode(invite_ok.index)?;
                sender.send(Encode(invite_ok.chunk, invite_ok.index, fragment))
            }));
        }
        if let Some(state) = self.persists.get_mut(&invite_ok.chunk) {
            if invite_ok.index == state.index {
                // store duplicated fragment is permitted but against the work's idea
                // should happen very rare anyway
                return Ok(());
            }
            // TODO load fragment and send
        }
        Ok(())
    }
}

impl<K> OnEvent<Encode> for Peer<K> {
    fn on_event(
        &mut self,
        Encode(chunk, index, fragment): Encode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.uploads.get(&chunk) else {
            return Ok(());
        };
        // TODO code path for serving from persistent state
        let Some(peer_id) = state.pending.get(&index) else {
            // is this ok?
            return Ok(());
        };
        let send_fragment = SendFragment {
            chunk,
            index,
            peer_id: Some(self.id),
        };
        let cancel = state.cancel.clone();
        self.blob.send(Transfer(
            *peer_id,
            send_fragment,
            Box::new(move |mut stream| {
                Box::pin(async move {
                    if tokio::select! {
                        result = stream.write_all(&fragment) => result,
                        // concurrent sending finished on some other n peers
                        () = cancel.cancelled() => return Ok(()),
                    }
                    // possible due to the invited peer has recovered from other concurrent sendings
                    .is_err()
                    {
                        //
                    }
                    Ok(())
                })
            }),
        ))
    }
}

impl<K> OnEvent<RecvBlob<SendFragment>> for Peer<K> {
    fn on_event(
        &mut self,
        RecvBlob(send_fragment, stream): RecvBlob<SendFragment>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(state) = self.downloads.get_mut(&send_fragment.chunk) {
            // println!("recv blob {}", H256(send_fragment.chunk));
            return self.fs.send(fs::Download(
                send_fragment.chunk,
                send_fragment.index,
                stream,
                state.recover.cancel.clone(),
            ));
        }
        if let Some(state) = self.persists.get_mut(&send_fragment.chunk) {
            let PersistStatus::Recovering(recover) = &mut state.status else {
                if let Some(peer_id) = send_fragment.peer_id {
                    assert_eq!(send_fragment.index, state.index);
                    let fragment_available = FragmentAvailable {
                        chunk: send_fragment.chunk,
                        peer_id: self.id,
                        peer_key: self.crypto.public_key(),
                    };
                    self.net
                        .send(peer_id, self.crypto.sign(fragment_available))?
                }
                return Ok(());
            };
            if send_fragment.index == state.index {
                assert!(state.notify.is_none());
                let peer_id = send_fragment
                    .peer_id
                    .ok_or(anyhow::anyhow!("expected peer id in SendFragment"))?;
                state.notify = Some(peer_id);
            }
            return self.fs.send(fs::Download(
                send_fragment.chunk,
                send_fragment.index,
                stream,
                recover.cancel.clone(),
            ));
        }
        Ok(())
    }
}

impl<K> OnEvent<fs::DownloadOk> for Peer<K> {
    fn on_event(
        &mut self,
        fs::DownloadOk(chunk, index, fragment): fs::DownloadOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if fragment.len() != self.fragment_len as usize {
            return Ok(()); // incomplete sending, well, not very elegant
        }
        if let Some(state) = self.downloads.get_mut(&chunk) {
            // println!("download {} index {index}", H256(chunk));
            return state
                .recover
                .submit_decode(chunk, index, fragment, None, &self.codec_worker);
        }
        if let Some(state) = self.persists.get_mut(&chunk) {
            let PersistStatus::Recovering(recover) = &mut state.status else {
                return Ok(());
            };
            if index == state.index {
                state.status = PersistStatus::Storing;
                self.fs.send(fs::Store(chunk, state.index, fragment))?
            } else if recover.received.insert(index) {
                recover.submit_decode(
                    chunk,
                    index,
                    fragment,
                    Some(state.index),
                    &self.codec_worker,
                )?
            } // otherwise it's either decoded or pending
            return Ok(());
        }
        Ok(())
    }
}

impl RecoverState {
    fn submit_decode(
        &mut self,
        chunk: Chunk,
        index: u32,
        fragment: Vec<u8>,
        encode_index: Option<u32>,
        worker: &CodecWorker,
    ) -> anyhow::Result<()> {
        if let Some(mut decoder) = self.decoder.take() {
            // println!("submit decode {} index {index}", H256(chunk));
            worker.submit(Box::new(move |(), sender| {
                if !decoder.decode(index, &fragment)? {
                    sender.send(Decode(chunk, decoder))
                } else if let Some(index) = encode_index {
                    let fragment = Encoder::try_from(decoder)?.encode(index)?;
                    sender.send(RecoverEncode(chunk, fragment))
                } else {
                    // recover does not return error when there's no sufficient block decoded??
                    sender.send(Recover(chunk, decoder.recover()?))
                }
            }))
        } else {
            self.pending.insert(index, fragment);
            Ok(())
        }
    }
}

impl<K> OnEvent<fs::StoreOk> for Peer<K> {
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
        if let Some(peer_id) = state.notify.take() {
            let fragment_available = FragmentAvailable {
                chunk,
                peer_id: self.id,
                peer_key: self.crypto.public_key(),
            };
            self.net
                .send(peer_id, self.crypto.sign(fragment_available))?
        }
        // TODO setup rereplicate timer
        Ok(())
    }
}

impl<K> OnEvent<Recv<Verifiable<FragmentAvailable>>> for Peer<K> {
    fn on_event(
        &mut self,
        Recv(fragment_available): Recv<Verifiable<FragmentAvailable>>,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.uploads.get_mut(&fragment_available.chunk) else {
            return Ok(());
        };
        if self
            .crypto
            .verify_with_public_key(
                Some(fragment_available.peer_id),
                &fragment_available.peer_key,
                &fragment_available,
            )
            .is_err()
        {
            // TODO log
            return Ok(());
        }
        state.available.insert(fragment_available.peer_id);
        if state.available.len() == self.chunk_n.into() {
            let state = self.uploads.remove(&fragment_available.chunk).unwrap();
            state.cancel.cancel();
            self.upcall.send(PutOk(state.preimage))?
        }
        Ok(())
    }
}

impl<K: Preimage> OnEvent<Get<K>> for Peer<K> {
    fn on_event(&mut self, Get(preimage): Get<K>, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        let chunk = preimage.target();
        let replaced = self.downloads.insert(
            chunk,
            DownloadState {
                preimage,
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
        // println!("multicast Pull {}", H256(chunk));
        self.net.send(Multicast(chunk, self.chunk_m), pull)
    }
}

impl<K> OnEvent<Recv<Pull>> for Peer<K> {
    fn on_event(&mut self, Recv(pull): Recv<Pull>, _: &mut impl Timer<Self>) -> anyhow::Result<()> {
        let Some(state) = self.persists.get(&pull.chunk) else {
            // println!("recv Pull {} (unknown)", H256(pull.chunk));
            return Ok(());
        };
        if !matches!(state.status, PersistStatus::Available) {
            // println!("recv Pull {} (unavailable)", H256(pull.chunk));
            return Ok(());
        }
        self.pending_pulls
            .entry(pull.chunk)
            .or_default()
            .push(pull.peer_id);
        // println!("recv Pull {}", H256(pull.chunk));
        self.fs.send(fs::Load(pull.chunk, state.index, true))
    }
}

impl<K> OnEvent<fs::LoadOk> for Peer<K> {
    fn on_event(
        &mut self,
        fs::LoadOk(chunk, index, fragment): fs::LoadOk,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(pending) = self.pending_pulls.remove(&chunk) else {
            return Ok(());
        };
        let send_fragment = SendFragment {
            chunk,
            index,
            peer_id: None,
        };
        let fragment = Bytes::from(fragment);
        for peer_id in pending {
            let fragment = fragment.clone();
            // println!("blob transfer {}", H256(chunk));
            self.blob.send(Transfer(
                peer_id,
                send_fragment.clone(),
                Box::new(move |mut stream| {
                    Box::pin(async move {
                        // possible due to the pulling peer has recovered from other concurrent
                        // sendings
                        if stream.write_all(&fragment).await.is_err() {
                            //
                        }
                        Ok(())
                    })
                }),
            ))?
        }
        Ok(())
    }
}

impl<K> OnEvent<Decode> for Peer<K> {
    fn on_event(
        &mut self,
        Decode(chunk, decoder): Decode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        if let Some(state) = self.downloads.get_mut(&chunk) {
            // println!("decode {}", H256(chunk));
            return state
                .recover
                .on_decode(chunk, decoder, None, &self.codec_worker);
        }
        if let Some(state) = self.persists.get_mut(&chunk) {
            let PersistStatus::Recovering(recover) = &mut state.status else {
                unreachable!()
            };
            return recover.on_decode(chunk, decoder, Some(state.index), &self.codec_worker);
        }
        Ok(())
    }
}

impl RecoverState {
    fn on_decode(
        &mut self,
        chunk: Chunk,
        decoder: Decoder,
        encode_index: Option<u32>,
        worker: &CodecWorker,
    ) -> anyhow::Result<()> {
        let replaced = self.decoder.replace(decoder);
        assert!(replaced.is_none());
        if let Some(&index) = self.pending.keys().next() {
            // println!("continue decode {} index {index}", H256(chunk));
            let fragment = self.pending.remove(&index).unwrap();
            self.submit_decode(chunk, index, fragment, encode_index, worker)?
        }
        Ok(())
    }
}

impl<K> OnEvent<Recover> for Peer<K> {
    fn on_event(
        &mut self,
        Recover(chunk, buf): Recover,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        // println!("recover {}", H256(chunk));
        if let Some(state) = self.downloads.remove(&chunk) {
            state.recover.cancel.cancel();
            self.upcall.send(GetOk(state.preimage, buf))
        } else {
            Ok(())
        }
    }
}

impl<K> OnEvent<RecoverEncode> for Peer<K> {
    fn on_event(
        &mut self,
        RecoverEncode(chunk, fragment): RecoverEncode,
        _: &mut impl Timer<Self>,
    ) -> anyhow::Result<()> {
        let Some(state) = self.persists.get_mut(&chunk) else {
            return Ok(()); // is this ok?
        };
        let replaced_status = replace(&mut state.status, PersistStatus::Storing);
        let PersistStatus::Recovering(recover) = replaced_status else {
            unreachable!()
        };
        recover.cancel.cancel();
        self.fs.send(fs::Store(chunk, state.index, fragment))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, derive_more::From)]
pub enum Message<A> {
    Invite(Invite),
    InviteOk(InviteOk),
    Pull(Pull),
    FragmentAvailable(Verifiable<FragmentAvailable>),

    FindPeer(Verifiable<FindPeer<A>>),
    FindPeerOk(Verifiable<FindPeerOk<A>>),

    BlobServe(Serve<SendFragment>),
}

pub type MessageNet<T, A> = augustus::net::MessageNet<T, Message<A>>;

pub trait SendRecvEvent:
    SendEvent<Recv<Invite>>
    + SendEvent<Recv<InviteOk>>
    + SendEvent<Recv<Pull>>
    + SendEvent<Recv<Verifiable<FragmentAvailable>>>
{
}
impl<
        T: SendEvent<Recv<Invite>>
            + SendEvent<Recv<InviteOk>>
            + SendEvent<Recv<Pull>>
            + SendEvent<Recv<Verifiable<FragmentAvailable>>>,
    > SendRecvEvent for T
{
}

pub fn on_buf<A: Addr>(
    buf: &[u8],
    entropy_sender: &mut impl SendRecvEvent,
    kademlia_sender: &mut impl kademlia::SendRecvEvent<A>,
    blob_sender: &mut impl blob::stream::SendRecvEvent<SendFragment>,
) -> anyhow::Result<()> {
    match deserialize(buf)? {
        Message::Invite(message) => entropy_sender.send(Recv(message)),
        Message::InviteOk(message) => entropy_sender.send(Recv(message)),
        Message::Pull(message) => entropy_sender.send(Recv(message)),
        Message::FragmentAvailable(message) => entropy_sender.send(Recv(message)),
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
        io::AsyncReadExt as _,
        net::TcpStream,
        sync::mpsc::UnboundedReceiver,
        task::JoinSet,
    };
    use tokio_util::sync::CancellationToken;

    use crate::Chunk;

    #[derive(Clone)]
    pub struct Store(pub Chunk, pub u32, pub Vec<u8>);

    impl Debug for Store {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("Store")
                .field("chunk", &H256(self.0))
                .field("index", &self.1)
                .field("data", &format!("<{} bytes>", self.2.len()))
                .finish()
        }
    }

    // Load(chunk, index, true) will delete fragment file while loading
    // not particual useful in practice, but good for evaluation with bounded storage usage
    #[derive(Debug, Clone)]
    pub struct Load(pub Chunk, pub u32, pub bool);

    // well, technically this is not a file system event
    // added after most code already done, and don't bother add another async session= =
    #[derive(Debug)]
    pub struct Download(pub Chunk, pub u32, pub TcpStream, pub CancellationToken);

    #[derive(Debug, Clone)]
    pub struct StoreOk(pub Chunk);

    #[derive(Clone)]
    pub struct LoadOk(pub Chunk, pub u32, pub Vec<u8>);

    impl Debug for LoadOk {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("LoadOk")
                .field("chunk", &H256(self.0))
                .field("index", &self.1)
                .field("data", &format!("<{} bytes>", self.2.len()))
                .finish()
        }
    }

    #[derive(Clone)]
    pub struct DownloadOk(pub Chunk, pub u32, pub Vec<u8>);

    impl Debug for DownloadOk {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.debug_struct("DownloadOk")
                .field("chunk", &H256(self.0))
                .field("index", &self.1)
                .field("data", &format!("<{} bytes>", self.2.len()))
                .finish()
        }
    }

    #[derive(Debug, derive_more::From)]
    pub enum Event {
        Store(Store),
        Load(Load),
        Download(Download),
    }

    pub trait Upcall: SendEvent<StoreOk> + SendEvent<LoadOk> + SendEvent<DownloadOk> {}
    impl<T: SendEvent<StoreOk> + SendEvent<LoadOk> + SendEvent<DownloadOk>> Upcall for T {}

    pub async fn session(
        path: impl AsRef<Path>,
        mut events: UnboundedReceiver<Event>,
        mut upcall: impl Upcall,
    ) -> anyhow::Result<()> {
        let mut store_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut load_tasks = JoinSet::<anyhow::Result<_>>::new();
        let mut download_tasks = JoinSet::<anyhow::Result<_>>::new();
        loop {
            enum Select {
                Recv(Event),
                JoinNextStore(Chunk),
                JoinNextLoad((Chunk, u32, Vec<u8>)),
                JoinNextDownload(Option<(Chunk, u32, Vec<u8>)>),
            }
            match tokio::select! {
                event = events.recv() => Select::Recv(event.ok_or(anyhow::anyhow!("channel closed"))?),
                Some(result) = store_tasks.join_next() => Select::JoinNextStore(result??),
                Some(result) = load_tasks.join_next() => Select::JoinNextLoad(result??),
                Some(result) = download_tasks.join_next() => Select::JoinNextDownload(result??),
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
                Select::Recv(Event::Download(Download(chunk, index, mut stream, cancel))) => {
                    download_tasks.spawn(async move {
                        let mut fragment = Vec::new();
                        let result = tokio::select! {
                            result = stream.read_to_end(&mut fragment) => result,
                            () = cancel.cancelled() => return Ok(None),
                        };
                        // Ok(None) if sender cancel the sending
                        Ok(result.ok().map(|_| (chunk, index, fragment)))
                    });
                }
                Select::JoinNextStore(chunk) => upcall.send(StoreOk(chunk))?,
                Select::JoinNextLoad((chunk, index, fragment)) => {
                    upcall.send(LoadOk(chunk, index, fragment))?
                }
                Select::JoinNextDownload(None) => {}
                Select::JoinNextDownload(Some((chunk, index, fragment))) => {
                    upcall.send(DownloadOk(chunk, index, fragment))?
                }
            }
        }
    }
}
