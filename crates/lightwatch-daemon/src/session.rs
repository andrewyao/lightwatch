//! One running program, seen through however many emitters are pointed at it.
//!
//! A target instrumented for both halves is two streams as far as [`store`] is
//! concerned, and has to stay that way: each emitter counts `seq` from its own
//! zero, and interleaving the two into one ring is what the store's
//! missed-frame accounting cannot survive. [`crate::store::ProcessId`] carries
//! the source for that reason.
//!
//! So nothing here is stored. [`pair_sessions`] is a pure fold over the
//! registry, recomputed per request, and the registry stays the only thing
//! that remembers anything.
//!
//! [`store`]: crate::store

use std::collections::BTreeMap;

use crate::store::{ProcessId, ProcessState, Registry};

/// The emitter that counts live objects. Every other source is a timing feed:
/// a census can only be taken from inside the target's address space, so a
/// bridge over an external profiler never produces one.
pub const CENSUS_SOURCE: &str = "lightwatch-probe";

/// How far apart two emitters' idea of one process's start may be before they
/// are taken to be describing two different runs of a recycled pid.
///
/// The probe reads `SystemTime::now()` as it starts; the bridge estimates
/// `now - status.elapsed` from a poll of the target's HTTP endpoint. They
/// never agree exactly, and the bridge's estimate moves by a few milliseconds
/// between its own runs, so an exact match would pair nothing at all.
pub const SAME_RUN_TOLERANCE_MS: u64 = 10_000;

/// Which half of the picture a process entry supplies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Feed {
    /// Per-function calls and durations.
    Cpu,
    /// Per-type live counts and bytes.
    Memory,
}

impl Feed {
    pub fn of(source: &str) -> Feed {
        if source == CENSUS_SOURCE {
            Feed::Memory
        } else {
            Feed::Cpu
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Feed::Cpu => "cpu",
            Feed::Memory => "memory",
        }
    }
}

/// One process entry, as a session sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub process_id: ProcessId,
    pub feed: Feed,
    pub source: String,
    pub started_unix_ms: u64,
    pub ended_unix_ms: Option<u64>,
    pub connected: bool,
    pub last_frame_unix_ms: Option<u64>,
}

/// One run of one program, with the emitters watching it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    pub id: String,
    pub app: String,
    pub pid: u32,
    pub cpu: Option<Member>,
    pub memory: Option<Member>,
    /// Entries that matched this session but lost to a better one of their own
    /// feed. Restarting a bridge against a still-running target leaves one of
    /// these behind, and dropping it silently would make the daemon look like
    /// it had picked at random.
    pub superseded: Vec<Member>,
}

impl Session {
    pub fn is_connected(&self) -> bool {
        let live = |m: &Option<Member>| m.as_ref().is_some_and(|m| m.connected);
        live(&self.cpu) || live(&self.memory)
    }

    /// Both feeds, in a fixed order, skipping the half that is missing.
    pub fn members(&self) -> impl Iterator<Item = &Member> {
        self.cpu.iter().chain(self.memory.iter())
    }

    fn started_unix_ms(&self) -> u64 {
        self.members().map(|m| m.started_unix_ms).min().unwrap_or(0)
    }
}

/// Every session the registry currently describes, newest run first.
///
/// Pure: it reads the registry and builds a fresh answer. Nothing is cached,
/// so a session appears the moment its second emitter connects and needs no
/// invalidation when one goes away.
pub fn pair_sessions(registry: &Registry) -> Vec<Session> {
    let mut by_target: BTreeMap<(String, u32), Vec<Member>> = BTreeMap::new();
    for state in registry.all() {
        let process = state.read().expect("process lock");
        by_target.entry((process.app.clone(), process.pid)).or_default().push(member(&process));
    }

    let mut sessions: Vec<Session> = by_target
        .into_iter()
        .flat_map(|((app, pid), members)| {
            runs(members).into_iter().map(move |run| session(app.clone(), pid, run))
        })
        .collect();
    sessions.sort_by(|a, b| b.started_unix_ms().cmp(&a.started_unix_ms()).then(a.id.cmp(&b.id)));
    sessions
}

pub fn find(registry: &Registry, id: &str) -> Option<Session> {
    pair_sessions(registry).into_iter().find(|session| session.id == id)
}

fn member(process: &ProcessState) -> Member {
    Member {
        process_id: process.id.clone(),
        feed: Feed::of(&process.source),
        source: process.source.clone(),
        started_unix_ms: process.started_unix_ms,
        ended_unix_ms: process.ended_unix_ms,
        connected: process.is_connected(),
        last_frame_unix_ms: process.last_frame_unix_ms,
    }
}

/// Splits one pid's entries into runs. A pid the OS handed out again is a
/// different program as far as anyone reading this is concerned, and the only
/// evidence for that is a start time far from the others.
fn runs(mut members: Vec<Member>) -> Vec<Vec<Member>> {
    members.sort_by_key(|m| m.started_unix_ms);
    let mut runs: Vec<Vec<Member>> = Vec::new();
    for member in members {
        match runs.last_mut() {
            Some(run)
                if member.started_unix_ms - run[run.len() - 1].started_unix_ms
                    <= SAME_RUN_TOLERANCE_MS =>
            {
                run.push(member)
            }
            _ => runs.push(vec![member]),
        }
    }
    runs
}

fn session(app: String, pid: u32, run: Vec<Member>) -> Session {
    let anchor = run.iter().map(|m| m.started_unix_ms).min().unwrap_or(0);
    let mut superseded = Vec::new();
    let mut cpu = None;
    let mut memory = None;
    for member in run {
        let slot = match member.feed {
            Feed::Cpu => &mut cpu,
            Feed::Memory => &mut memory,
        };
        match slot.take() {
            Some(held) if outranks(&held, &member) => {
                superseded.push(member);
                *slot = Some(held);
            }
            Some(held) => {
                superseded.push(held);
                *slot = Some(member);
            }
            None => *slot = Some(member),
        }
    }
    superseded.sort_by_key(|member| std::cmp::Reverse(member.started_unix_ms));
    Session { id: session_id(&app, pid, anchor), app, pid, cpu, memory, superseded }
}

/// Which of two entries for the same feed speaks for the session. A live
/// emitter beats a dead one; between two live ones the newest wins, because a
/// restarted bridge leaves the stream it abandoned marked connected until its
/// socket closes.
fn outranks(held: &Member, candidate: &Member) -> bool {
    (held.connected, held.started_unix_ms) >= (candidate.connected, candidate.started_unix_ms)
}

/// Stable across requests, and safe in a path segment whatever the target
/// called itself.
fn session_id(app: &str, pid: u32, anchor_unix_ms: u64) -> String {
    let app: String = app
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '-' || c == '_' { c } else { '_' })
        .collect();
    format!("{app}-{pid}-{anchor_unix_ms}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use lightwatch_proto::Hello;

    const PROBE: &str = CENSUS_SOURCE;
    const BRIDGE: &str = "lightwatch-hotpath";

    fn registry_with(emitters: &[(&str, &str, u32, u64)]) -> Registry {
        let registry = Registry::new();
        for (app, source, pid, started) in emitters {
            registry.connect(&Hello::new(*app, *source, *pid, *started));
        }
        registry
    }

    #[test]
    fn the_two_emitters_watching_one_pid_become_one_session() {
        let registry = registry_with(&[
            ("lightphotos", PROBE, 64376, 1_790_000_000_000),
            ("lightphotos", BRIDGE, 64376, 1_790_000_000_137),
        ]);

        let sessions = pair_sessions(&registry);
        assert_eq!(sessions.len(), 1, "one program is one session, not two");
        let session = &sessions[0];
        assert_eq!(session.pid, 64376);
        assert_eq!(session.cpu.as_ref().map(|m| m.source.as_str()), Some(BRIDGE));
        assert_eq!(session.memory.as_ref().map(|m| m.source.as_str()), Some(PROBE));
        assert!(session.superseded.is_empty());
    }

    #[test]
    fn a_restarted_bridge_supersedes_its_own_abandoned_entry_instead_of_forking_the_session() {
        // The bridge estimates start as `now - elapsed`, and that estimate
        // drifts between runs, so the same live target gets a second entry.
        let registry = registry_with(&[
            ("lightphotos", PROBE, 64376, 1_790_000_000_000),
            ("lightphotos", BRIDGE, 64376, 1_790_000_000_137),
        ]);
        registry.disconnect(&ProcessId::of(64376, 1_790_000_000_137, BRIDGE));
        registry.connect(&Hello::new("lightphotos", BRIDGE, 64376, 1_790_000_000_152));

        let sessions = pair_sessions(&registry);
        assert_eq!(sessions.len(), 1, "a second bridge run is not a second session");
        let session = &sessions[0];
        let cpu = session.cpu.as_ref().expect("a cpu feed");
        assert_eq!(cpu.started_unix_ms, 1_790_000_000_152, "the connected entry wins");
        assert!(cpu.connected);
        assert_eq!(
            session.superseded.iter().map(|m| m.started_unix_ms).collect::<Vec<_>>(),
            vec![1_790_000_000_137],
            "the entry that lost is reported, not dropped"
        );
    }

    #[test]
    fn between_two_connected_entries_of_one_feed_the_newest_speaks() {
        let registry = registry_with(&[
            ("lightphotos", BRIDGE, 64376, 1_790_000_000_137),
            ("lightphotos", BRIDGE, 64376, 1_790_000_000_152),
        ]);
        let sessions = pair_sessions(&registry);
        let cpu = sessions[0].cpu.as_ref().expect("a cpu feed");
        assert_eq!(cpu.started_unix_ms, 1_790_000_000_152);
        assert_eq!(sessions[0].superseded.len(), 1);
    }

    #[test]
    fn a_pid_the_os_handed_out_again_is_a_different_session() {
        let registry = registry_with(&[
            ("lightphotos", PROBE, 64376, 1_790_000_000_000),
            ("lightphotos", PROBE, 64376, 1_790_000_060_000),
        ]);
        let sessions = pair_sessions(&registry);
        assert_eq!(sessions.len(), 2, "a minute apart is not one run");
        assert_ne!(sessions[0].id, sessions[1].id);
        assert!(sessions[0].superseded.is_empty());
    }

    #[test]
    fn two_programs_sharing_a_pid_across_machines_do_not_pair() {
        let registry = registry_with(&[
            ("lightphotos", PROBE, 64376, 1_790_000_000_000),
            ("demo", BRIDGE, 64376, 1_790_000_000_100),
        ]);
        let sessions = pair_sessions(&registry);
        assert_eq!(sessions.len(), 2);
        assert!(sessions.iter().all(|s| s.cpu.is_none() || s.memory.is_none()));
    }

    #[test]
    fn a_target_with_only_one_emitter_is_still_a_session() {
        let registry = registry_with(&[("lightphotos", PROBE, 64376, 1_790_000_000_000)]);
        let sessions = pair_sessions(&registry);
        assert_eq!(sessions.len(), 1);
        assert!(sessions[0].cpu.is_none(), "half a picture is what there is");
        assert!(sessions[0].memory.is_some());
    }

    #[test]
    fn a_session_id_survives_an_app_name_that_would_break_a_url() {
        let registry = registry_with(&[("my app/v2", PROBE, 7, 1_790_000_000_000)]);
        let sessions = pair_sessions(&registry);
        assert_eq!(sessions[0].id, "my_app_v2-7-1790000000000");
        assert_eq!(sessions[0].app, "my app/v2", "the real name is still reported");
        assert_eq!(find(&registry, &sessions[0].id), Some(sessions[0].clone()));
    }
}
