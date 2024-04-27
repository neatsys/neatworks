use std::{fmt::Write, time::SystemTime};

use augustus::{
    boson::{nitro_enclaves_portal_session, NitroEnclavesClock, Update, UpdateOk},
    cops::DefaultVersion,
};
use tokio::{
    sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    time::Instant,
};

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
        let mut lines = String::new();
        for size in (0..=16).step_by(2).map(|n| 1 << n) {
            bench_session(size, 0, &update_sender, &mut update_ok_receiver, &mut lines).await?
        }
        for num_merged in 0..=15 {
            bench_session(
                1 << 10,
                num_merged,
                &update_sender,
                &mut update_ok_receiver,
                &mut lines,
            )
            .await?
        }
        tokio::fs::write(
            format!(
                "clock-nitro-enclaves-{}.txt",
                SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs()
            ),
            lines,
        )
        .await?;
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

async fn bench_session(
    size: usize,
    num_merged: usize,
    update_sender: &UnboundedSender<Update<NitroEnclavesClock>>,
    update_ok_receiver: &mut UnboundedReceiver<UpdateOk<NitroEnclavesClock>>,
    lines: &mut String,
) -> anyhow::Result<()> {
    let clock =
        NitroEnclavesClock::try_from(DefaultVersion((0..size).map(|i| (i as _, 0)).collect()))?;
    update_sender.send(Update(clock, Default::default(), 0))?;
    let Some((_, clock)) = update_ok_receiver.recv().await else {
        anyhow::bail!("missing UpdateOk")
    };
    for _ in 0..10 {
        let update = Update(clock.clone(), vec![clock.clone(); num_merged], 0);
        let start = Instant::now();
        update_sender.send(update)?;
        let Some((_, clock)) = update_ok_receiver.recv().await else {
            anyhow::bail!("missing UpdateOk")
        };
        let elapsed = start.elapsed();
        println!("{size:8} {num_merged:3} {elapsed:?}");
        writeln!(lines, "{size},{num_merged},{}", elapsed.as_secs_f32())?;
        let document = clock.verify()?;
        anyhow::ensure!(document.is_some())
    }
    Ok(())
}
