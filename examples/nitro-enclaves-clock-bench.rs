use std::env::args;

use augustus::{
    boson::{nitro_enclaves_portal_session, NitroEnclavesClock, Update, UpdateOk},
    cops::DefaultVersion,
};
use tokio::{sync::mpsc::unbounded_channel, time::Instant};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cid = args()
        .nth(1)
        .ok_or(anyhow::format_err!("missing cid"))?
        .parse()?;
    let (update_sender, update_receiver) = unbounded_channel();
    let (update_ok_sender, mut update_ok_receiver) = unbounded_channel::<UpdateOk<_>>();
    let portal_session = tokio::spawn(nitro_enclaves_portal_session(
        cid,
        update_receiver,
        update_ok_sender,
    ));
    let session = async move {
        let start = Instant::now();
        update_sender.send(Update(
            NitroEnclavesClock::try_from(DefaultVersion::default())?,
            Default::default(),
            0,
        ))?;
        let Some((_, clock)) = update_ok_receiver.recv().await else {
            anyhow::bail!("missing UpdateOk")
        };
        println!("{:?}", start.elapsed());
        let document = clock.verify()?;
        println!("{document:?}");
        anyhow::Ok(())
    };
    'select: {
        tokio::select! {
            result = session => break 'select result?,
            result = portal_session => result??,
        }
        anyhow::bail!("unreachable")
    }
    Ok(())
}
