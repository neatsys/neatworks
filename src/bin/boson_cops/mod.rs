use std::net::SocketAddr;

use augustus::{
    app::{self, ycsb, App},
    crypto::{Crypto, CryptoFlavor},
    event::{
        self,
        erased::{session::Sender, Blanket, Buffered, Session, Unify},
        Once,
    },
    net::{
        dispatch,
        session::{tcp, Tcp},
        Dispatch, IndexNet,
    },
    pbft,
    worker::{Submit, Worker},
    workload::{CloseLoop, Iter, Json, Upcall, Workload},
};
use boson_control_messages::{CopsClient, CopsServer, CopsVariant};
use rand::{rngs::StdRng, thread_rng, SeedableRng};
use rand_distr::Uniform;
use tokio::{net::TcpListener, task::JoinSet};
use tokio_util::sync::CancellationToken;

pub async fn pbft_client_session(config: CopsClient) -> anyhow::Result<()> {
    let CopsClient {
        addrs,
        ip,
        num_concurrent,
        num_concurrent_put,
        record_count,
        put_range,
        variant: CopsVariant::Replicated(config),
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let num_replica = addrs.len();
    let num_faulty = config.num_faulty;

    let mut sessions = JoinSet::new();
    for index in 0..num_concurrent {
        let tcp_listener = TcpListener::bind((ip, 0)).await?;
        let addr = tcp_listener.local_addr()?;
        let do_put = index < num_concurrent_put;
        // caution: non shared workload only for without insertion
        let mut workload = ycsb::Workload::new(
            StdRng::seed_from_u64(117418 + index as u64),
            if do_put {
                ycsb::WorkloadSettings::new_a
            } else {
                ycsb::WorkloadSettings::new_c
            }(record_count),
        )?;
        if do_put {
            workload.key_num = ycsb::Gen::Uniform(Uniform::from(put_range.clone()))
        }

        let mut dispatch_session = event::Session::new();
        let mut client_session = Session::new();
        let mut close_loop_session = Session::new();

        let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
            Tcp::new(addr)?,
            {
                let mut sender = Sender::from(client_session.sender());
                move |buf: &_| pbft::to_client_on_buf(buf, &mut sender)
            },
            Once(dispatch_session.sender()),
        )?));
        let mut client = Blanket(Buffered::from(pbft::Client::new(
            rand::random(),
            addr,
            pbft::ToReplicaMessageNet::new(IndexNet::new(
                dispatch::Net::from(dispatch_session.sender()),
                addrs.clone(),
                None,
            )),
            Box::new(Sender::from(close_loop_session.sender())) as Box<dyn Upcall + Send + Sync>,
            num_replica,
            num_faulty,
        )));
        let mut close_loop = Blanket(Unify(CloseLoop::new(
            Sender::from(client_session.sender()),
            Json(workload),
        )));

        sessions.spawn(async move {
            let dispatch_session = dispatch_session.run(&mut dispatch);
            let client_session = client_session.run(&mut client);
            let close_loop_session = async move {
                //
                close_loop_session.run(&mut close_loop).await?;
                Ok(())
            };
            tokio::select! {
                result = dispatch_session => result?,
                result = client_session => result?,
                result = close_loop_session => return result,
            }
            anyhow::bail!("unreachable")
        });
    }
    Ok(())
}

pub async fn pbft_server_session(
    config: CopsServer,
    cancel: CancellationToken,
) -> anyhow::Result<()> {
    let CopsServer {
        addrs,
        id,
        record_count,
        variant: CopsVariant::Replicated(config),
    } = config
    else {
        anyhow::bail!("unimplemented")
    };
    let addr = addrs[id as usize];
    let num_replica = addrs.len();
    let num_faulty = config.num_faulty;
    let mut app = app::BTreeMap::new();
    {
        let mut workload =
            ycsb::Workload::new(thread_rng(), ycsb::WorkloadSettings::new(record_count))?;
        let mut workload = Json(Iter::<_, _, ()>::from(workload.startup_ops()));
        while let Some((op, ())) = workload.next_op()? {
            app.execute(&op)?;
        }
    }

    let tcp_listener = TcpListener::bind(addr).await?;
    let mut dispatch_session = event::Session::new();
    let mut replica_session = Session::new();

    let mut dispatch = event::Unify(event::Buffered::from(Dispatch::new(
        Tcp::new(addr)?,
        {
            let mut sender = Sender::from(replica_session.sender());
            move |buf: &_| pbft::to_replica_on_buf(buf, &mut sender)
        },
        Once(dispatch_session.sender()),
    )?));
    let mut replica = Blanket(Buffered::from(pbft::Replica::new(
        id,
        app,
        pbft::ToReplicaMessageNet::new(IndexNet::new(
            dispatch::Net::from(dispatch_session.sender()),
            addrs,
            id as usize,
        )),
        pbft::ToClientMessageNet::new(dispatch::Net::from(dispatch_session.sender())),
        Box::new(pbft::CryptoWorker::from(Worker::Inline(
            Crypto::new_hardcoded_replication(num_replica, id, CryptoFlavor::Schnorrkel)?,
            Sender::from(replica_session.sender()),
        ))) as Box<dyn Submit<Crypto, dyn pbft::SendCryptoEvent<SocketAddr>> + Send + Sync>,
        num_replica,
        num_faulty,
    )));

    let tcp_accept_session = tcp::accept_session(tcp_listener, dispatch_session.sender());
    let dispatch_session = dispatch_session.run(&mut dispatch);
    let replica_session = replica_session.run(&mut replica);
    tokio::select! {
        () = cancel.cancelled() => return Ok(()),
        result = tcp_accept_session => result?,
        result = dispatch_session => result?,
        result = replica_session => result?,
    }
    anyhow::bail!("unreachable")
}

// cSpell:words pbft
