use augustus::{
    boson::{nitro_enclaves_portal_session, NitroEnclavesClock, Update, UpdateOk},
    cops::DefaultVersion,
};
use tokio::{sync::mpsc::unbounded_channel, time::Instant};

const CID: u32 = 16;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let (update_sender, update_receiver) = unbounded_channel();
    let (update_ok_sender, mut update_ok_receiver) = unbounded_channel::<UpdateOk<_>>();
    let portal_session = tokio::spawn(nitro_enclaves_portal_session(
        CID,
        update_receiver,
        update_ok_sender,
    ));
    let session = async move {
        let mut clock = NitroEnclavesClock::try_from(DefaultVersion::default())?;
        for i in 0..4 {
            let start = Instant::now();
            update_sender.send(Update(clock, Default::default(), i))?;
            let Some((_, updated_clock)) = update_ok_receiver.recv().await else {
                anyhow::bail!("missing UpdateOk")
            };
            println!("{:?}", start.elapsed());
            let document = updated_clock.verify()?;
            anyhow::ensure!(document.is_some());
            // println!("{document:?}");
            clock = updated_clock
        }
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
