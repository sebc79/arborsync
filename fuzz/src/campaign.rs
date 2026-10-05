use std::collections::HashMap;
use std::path::Path;

use crate::oracle::{self, Intent, judge, project};
use crate::schedule::{Actor, Bin, Schedule, Step};
use crate::types::{Finding, Fingerprint, HarnessError, Verdict};
use crate::world::{Boot, FreezeToken, Wait, World, reap_abandoned};

pub(crate) enum Shrink {
    On,
    Off,
}

pub(crate) struct Drive {
    #[cfg(test)]
    pub intent: Intent,
    #[cfg(test)]
    pub observed: Option<oracle::Observed>,
    pub finding: Option<Finding>,
    pub steps_ran: usize,
}

impl Drive {
    fn finish(
        intent: Intent,
        observed: Option<oracle::Observed>,
        finding: Option<Finding>,
        steps_ran: usize,
    ) -> Self {
        #[cfg(not(test))]
        {
            drop(intent);
            drop(observed);
        }
        Self {
            #[cfg(test)]
            intent,
            #[cfg(test)]
            observed,
            finding,
            steps_ran,
        }
    }
}

pub(crate) fn drive(world: &mut World, schedule: &Schedule) -> Result<Drive, HarnessError> {
    let mut intent = Intent::new(world.layout());
    intent.set_yard(world.yard());
    let mut tokens: HashMap<Actor, FreezeToken> = HashMap::new();
    let mut observed = None;
    let mut epoch = 0u32;
    for (index, step) in schedule.steps().iter().enumerate() {
        let steps_ran = index + 1;
        match step {
            Step::Freeze(actor) => {
                let token = world.freeze(*actor)?;
                tokens.insert(*actor, token);
                intent.push(oracle::Event::Freeze(*actor));
            }
            Step::Thaw(actor) => {
                let token = tokens.remove(actor).ok_or_else(|| {
                    HarnessError::Sandbox(format!("{} has no freeze token", actor.label()))
                })?;
                world.thaw(token)?;
                intent.push(oracle::Event::Thaw(*actor));
            }
            Step::Disk(op) => {
                let token = tokens.get(&op.actor()).ok_or_else(|| {
                    HarnessError::Sandbox(format!("{} is not frozen", op.actor().label()))
                })?;
                let snap = world.apply(token, op)?;
                intent.push(oracle::Event::Disk {
                    op: op.clone(),
                    snap,
                });
            }
            Step::World(op) => match world.probe(op)? {
                Boot::Up => intent.push(oracle::Event::World(op.clone())),
                Boot::Failed(found) => {
                    return Ok(Drive::finish(intent, observed, Some(found), steps_ran));
                }
            },
            Step::Quiesce => match world.wait_idle()? {
                Wait::Idle(_) => intent.push(oracle::Event::Quiesce),
                Wait::Failed(found) => {
                    return Ok(Drive::finish(intent, observed, Some(found), steps_ran));
                }
            },
            Step::Settle => match world.wait_idle()? {
                Wait::Failed(found) => {
                    return Ok(Drive::finish(intent, observed, Some(found), steps_ran));
                }
                Wait::Idle(obs) => {
                    intent.push(oracle::Event::Settle);
                    world.seal_epoch(epoch)?;
                    epoch += 1;
                    let projection = project(&intent);
                    if let Some(found) = judge(&projection, &obs) {
                        return Ok(Drive::finish(intent, Some(obs), Some(found), steps_ran));
                    }
                    observed = Some(obs);
                }
            },
        }
    }
    Ok(Drive::finish(
        intent,
        observed,
        None,
        schedule.steps().len(),
    ))
}

enum Run {
    Clean,
    Found { found: Finding, ran: Schedule },
}

pub(crate) fn execute_with(
    bin: &Bin,
    schedule: Schedule,
    shrink: Shrink,
) -> Result<Verdict, HarnessError> {
    let outcome = run_once(bin, &schedule)?;
    match outcome {
        Run::Clean => Ok(Verdict::Clean),
        Run::Found { found, ran } => {
            let ran = if matches!(shrink, Shrink::On) {
                shrink_to(bin, ran, found.fingerprint())?
            } else {
                ran
            };
            let artifact = write_artifact(&ran, &found)?;
            Ok(Verdict::Finding { found, artifact })
        }
    }
}

pub fn execute(bin: &Bin, schedule: Schedule) -> Result<Verdict, HarnessError> {
    execute_with(bin, schedule, Shrink::On)
}

pub fn replay(bin: &Bin, artifact: &Path) -> Result<Verdict, HarnessError> {
    let bytes = std::fs::read(artifact).map_err(|err| {
        HarnessError::Artifact(format!("read {}: {err}", artifact.display()))
    })?;
    let schedule = Schedule::from_bytes(&bytes)?;
    execute_with(bin, schedule, Shrink::Off)
}

fn run_once(bin: &Bin, schedule: &Schedule) -> Result<Run, HarnessError> {
    reap_abandoned()?;
    let mut world = World::claim(bin, schedule.layout())?;
    match world.spawn()? {
        Boot::Failed(found) => {
            world.close();
            return Ok(Run::Found {
                found,
                ran: schedule.clone(),
            });
        }
        Boot::Up => {}
    }
    let driven = drive(&mut world, schedule)?;
    world.close();
    if let Some(found) = driven.finding {
        let ran = schedule
            .prefix(driven.steps_ran)
            .unwrap_or_else(|_| schedule.clone());
        Ok(Run::Found { found, ran })
    } else {
        Ok(Run::Clean)
    }
}

fn shrink_to(bin: &Bin, schedule: Schedule, fingerprint: Fingerprint) -> Result<Schedule, HarnessError> {
    let mut best = schedule;
    let mut index = 0;
    let mut attempts = 0;
    while index < best.steps().len() && attempts < 6 {
        let mut steps = best.steps().to_vec();
        steps.remove(index);
        attempts += 1;
        let Ok(candidate) = Schedule::try_from_parts(best.seed(), best.layout().clone(), steps) else {
            index += 1;
            continue;
        };
        match run_once(bin, &candidate)? {
            Run::Found { found, .. } if found.fingerprint() == fingerprint => {
                best = candidate;
            }
            _ => index += 1,
        }
    }
    Ok(best)
}

fn write_artifact(schedule: &Schedule, found: &Finding) -> Result<std::path::PathBuf, HarnessError> {
    let dir = std::path::PathBuf::from("findings");
    std::fs::create_dir_all(&dir).map_err(|err| HarnessError::Io(err.to_string()))?;
    let name = format!("{}-{}.json", found.kind(), schedule.seed());
    let final_path = dir.join(&name);
    let tmp = dir.join(format!(".{name}.tmp"));
    std::fs::write(&tmp, schedule.to_bytes()).map_err(|err| HarnessError::Io(err.to_string()))?;
    std::fs::rename(&tmp, &final_path).map_err(|err| HarnessError::Io(err.to_string()))?;
    Ok(final_path)
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::Mutex;

    use super::*;
    use crate::oracle::{judge, project};
    use crate::schedule::{
        Actor, DiskOp, Layout, Limits, MtimeNs, RelPath, Schedule, SlaveIx, Step, UnixMode, WorldOp,
    };
    use crate::world::{Boot, World};

    static CWD: Mutex<()> = Mutex::new(());

    struct RestoreDir(PathBuf);
    impl Drop for RestoreDir {
        fn drop(&mut self) {
            let _ = std::env::set_current_dir(&self.0);
        }
    }

    fn arborsync() -> Bin {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../target/debug/arborsync");
        Bin::new(path).expect("target/debug/arborsync")
    }

    fn hello_path() -> RelPath {
        RelPath::new(vec!["hello.txt".into()]).unwrap()
    }

    fn put_hello() -> DiskOp {
        DiskOp::Put {
            actor: Actor::Master,
            path: hello_path(),
            bytes: b"hello".to_vec(),
            mode: UnixMode::new(0o644).unwrap(),
            mtime: MtimeNs::new(1_700_000_000_000_000_000),
        }
    }

    fn in_temp_cwd() -> (RestoreDir, std::sync::MutexGuard<'static, ()>) {
        let lock = CWD.lock().unwrap_or_else(|err| err.into_inner());
        let dir = std::env::temp_dir().join(format!(
            "arborsync-fuzz-cwd-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        fs::create_dir_all(&dir).unwrap();
        let prev = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        (RestoreDir(prev), lock)
    }

    #[test]
    fn live_put_replicates_hello_before_close() {
        let (_cwd, _lock) = in_temp_cwd();
        let schedule = Schedule::try_from_parts(
            1,
            Layout::One,
            vec![
                Step::Freeze(Actor::Master),
                Step::Disk(put_hello()),
                Step::Thaw(Actor::Master),
                Step::Settle,
            ],
        )
        .unwrap();
        let bin = arborsync();
        let mut world = World::claim(&bin, schedule.layout()).unwrap();
        match world.spawn().unwrap() {
            Boot::Up => {}
            Boot::Failed(found) => {
                let log = fs::read_to_string(world.log_path(Actor::Master)).unwrap_or_default();
                panic!("{found}\n{log}");
            }
        }
        let driven = drive(&mut world, &schedule).unwrap_or_else(|err| {
            let master = fs::read_to_string(world.log_path(Actor::Master)).unwrap_or_default();
            let slave = fs::read_to_string(world.log_path(Actor::Slave(
                SlaveIx::new(0, Layout::One.slave_count()).unwrap(),
            )))
            .unwrap_or_default();
            panic!("{err}\nmaster:\n{master}\nslave:\n{slave}");
        });
        let slave = Actor::Slave(SlaveIx::new(0, Layout::One.slave_count()).unwrap());
        let bytes = fs::read(world.tree_root(slave).join("hello.txt")).unwrap_or_else(|err| {
            let master = fs::read_to_string(world.log_path(Actor::Master)).unwrap_or_default();
            let slave_log = fs::read_to_string(world.log_path(slave)).unwrap_or_default();
            panic!("read hello: {err}\nmaster:\n{master}\nslave:\n{slave_log}\nfinding: {:?}", driven.finding);
        });
        assert_eq!(bytes, b"hello");
        let projection = project(&driven.intent);
        assert_eq!(
            judge(&projection, driven.observed.as_ref().expect("observed")),
            None
        );
        world.close();
    }

    #[test]
    fn live_retouch_same_stamp_keeps_previous_bytes() {
        let (_cwd, _lock) = in_temp_cwd();
        let schedule = Schedule::try_from_parts(
            2,
            Layout::One,
            vec![
                Step::Freeze(Actor::Master),
                Step::Disk(put_hello()),
                Step::Thaw(Actor::Master),
                Step::Settle,
                Step::Freeze(Actor::Master),
                Step::Disk(DiskOp::RetouchSameStamp {
                    actor: Actor::Master,
                    path: hello_path(),
                    bytes: b"HELLO".to_vec(),
                }),
                Step::Thaw(Actor::Master),
                Step::Settle,
            ],
        )
        .unwrap();
        let bin = arborsync();
        let mut world = World::claim(&bin, schedule.layout()).unwrap();
        match world.spawn().unwrap() {
            Boot::Up => {}
            Boot::Failed(found) => panic!("{found}"),
        }
        let driven = drive(&mut world, &schedule).unwrap();
        let slave = Actor::Slave(SlaveIx::new(0, Layout::One.slave_count()).unwrap());
        let bytes = fs::read(world.tree_root(slave).join("hello.txt")).unwrap_or_else(|err| {
            panic!(
                "read hello: {err} finding={:?} master={} slave={}",
                driven.finding,
                fs::read_to_string(world.log_path(Actor::Master)).unwrap_or_default(),
                fs::read_to_string(world.log_path(slave)).unwrap_or_default()
            );
        });
        assert_eq!(bytes, b"hello");
        let projection = project(&driven.intent);
        assert_eq!(
            judge(&projection, driven.observed.as_ref().expect("observed")),
            None,
            "verdict clean"
        );
        assert!(driven.finding.is_none(), "verdict clean");
        world.close();
    }

    #[test]
    fn from_seed_schedule_round_trips_for_the_campaign_shape() {
        let schedule = Schedule::from_seed(1, Limits::new(8, 1).unwrap());
        assert!(schedule.steps().iter().all(|step| {
            !matches!(
                step,
                Step::World(WorldOp::Grammar(_)) | Step::World(WorldOp::BadBulk { .. })
            )
        }));
    }
}
