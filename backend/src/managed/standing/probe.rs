use std::{collections::{HashMap, HashSet}, sync::{Arc, Mutex, OnceLock}};
use tokio::sync::Notify;
use uuid::Uuid;

#[derive(Clone)]
pub struct Barrier { reached: Arc<Notify>, release: Arc<Notify> }
impl Barrier {
    pub async fn reached(&self) { self.reached.notified().await; }
    pub fn release(&self) { self.release.notify_one(); }
}
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Stage { Claimed, MetadataMissing, BeforePrivateFence, BeforeRunningStatus, BeforeChargePrincipal, FundedFenceDenied, NoPayer, BeforePublication,
    /// Inside publication: the turn and the standing fence are held.
    PublicationFenced,
    /// Standing adoption, keyed by the source version: after the sweep read
    /// it as a candidate, before the locked repin.
    BeforeAdoptionRepin }
#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
pub enum Ack { Publication, QueueCompletion }
#[derive(Default)]
struct Probes {
    barriers: HashMap<(Uuid, Stage), Barrier>,
    drop_ack: HashSet<(Uuid, Ack)>,
    dropped: HashSet<(Uuid, Ack)>,
    sweep_clocks: HashMap<String,SweepClock>,
}
fn probes() -> &'static Mutex<Probes> {
    static PROBES: OnceLock<Mutex<Probes>> = OnceLock::new();
    PROBES.get_or_init(Default::default)
}
pub fn pause_at(job: Uuid, stage: Stage) -> Barrier {
    let barrier=Barrier { reached: Arc::new(Notify::new()), release: Arc::new(Notify::new()) };
    assert!(probes().lock().unwrap().barriers.insert((job,stage),barrier.clone()).is_none());
    barrier
}
pub(crate) async fn checkpoint(job: Uuid, stage: Stage) {
    let barrier=probes().lock().unwrap().barriers.remove(&(job,stage));
    if let Some(barrier)=barrier { barrier.reached.notify_one(); barrier.release.notified().await; }
}
pub fn drop_ack_once(job: Uuid, ack: Ack) { assert!(probes().lock().unwrap().drop_ack.insert((job,ack))); }
pub(crate) fn lose_ack(job: Uuid, ack: Ack) -> bool {
    let mut probes=probes().lock().unwrap();
    if probes.drop_ack.remove(&(job,ack)) { probes.dropped.insert((job,ack)); true } else { false }
}
pub fn ack_was_dropped(job: Uuid, ack: Ack) -> bool { probes().lock().unwrap().dropped.contains(&(job,ack)) }

struct SweepClock {
    base: tokio::time::Instant,
    elapsed: std::time::Duration,
    step: std::time::Duration,
    reset_position: bool,
    slots: Vec<(Uuid,String,bool)>,
    accounts: Vec<Uuid>,
}
pub fn configure_sweep_clock(database: &str, step: std::time::Duration) {
    assert!(step<std::time::Duration::from_secs(4));
    probes().lock().unwrap().sweep_clocks.insert(database.into(),SweepClock {
        base:tokio::time::Instant::now(),elapsed:std::time::Duration::ZERO,step,reset_position:true,slots:vec![],accounts:vec![],
    });
}
pub(crate) fn begin_sweep_clock(database: &str) -> bool {
    if let Some(clock)=probes().lock().unwrap().sweep_clocks.get_mut(database) {
        clock.base=tokio::time::Instant::now();clock.elapsed=std::time::Duration::ZERO;clock.slots.clear();clock.accounts.clear();
        std::mem::take(&mut clock.reset_position)
    } else { false }
}
pub(crate) fn sweep_instant(database: &str, actual: tokio::time::Instant) -> tokio::time::Instant {
    probes().lock().unwrap().sweep_clocks.get(database).map(|clock|clock.base+clock.elapsed).unwrap_or(actual)
}
pub(crate) fn advance_sweep_clock(database: &str, graph: Uuid, profile: &str, retired: bool) {
    if let Some(clock)=probes().lock().unwrap().sweep_clocks.get_mut(database) {
        clock.elapsed+=clock.step;clock.slots.push((graph,profile.into(),retired));
    }
}
pub fn sweep_slots(database: &str) -> Vec<(Uuid,String,bool)> {
    probes().lock().unwrap().sweep_clocks.get(database).unwrap().slots.clone()
}
pub(crate) fn visit_sweep_account(database: &str, account: Uuid) {
    if let Some(clock)=probes().lock().unwrap().sweep_clocks.get_mut(database) { clock.accounts.push(account); }
}
pub fn sweep_accounts(database: &str) -> Vec<Uuid> {
    probes().lock().unwrap().sweep_clocks.get(database).unwrap().accounts.clone()
}
pub fn clear_sweep_clock(database: &str) { probes().lock().unwrap().sweep_clocks.remove(database); }
