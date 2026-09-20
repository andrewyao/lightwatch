//! Turning successive hotpath reports into lightwatch frames.
//!
//! hotpath's counters are cumulative from process start. A window's activity is
//! the difference between two readings, so all the state the bridge keeps is
//! the last reading of every function id it has seen during one run of one
//! target process.
//!
//! Three properties of the source shape this, and each is handled here rather
//! than papered over at the edges:
//!
//! 1. `data` is truncated to a row limit, so a function that appeared once and
//!    is missing next time has not gone quiet. It fell off the list. Readings
//!    are therefore never evicted, and a missing id produces no event at all.
//! 2. Every number is a preformatted string. `total` is the only one worth
//!    differencing, so the window's mean duration is `delta_total / delta_calls`
//!    and that single value is what goes on the wire. The percentile fields
//!    describe the function's whole life, not the window, and inventing a
//!    distribution out of them would be a fiction the daemon could not detect.
//! 3. The target restarts. [`Stream::same_run`] is the guard; the caller opens
//!    a new stream rather than differencing across two processes.

use hotpath::json::{JsonFunctionEntry, JsonFunctionsList};
use lightwatch_proto::{bucket_of, Dist, Event, Frame, FunctionId, Location, Register};
use std::collections::HashMap;

/// The last cumulative reading for one function.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Reading {
    calls: u64,
    total_ns: u64,
}

/// The bridge's view of one run of one target process.
///
/// Dropped and rebuilt when the target restarts, which is also what makes it
/// safe to pass hotpath's own function ids straight through as
/// [`FunctionId`]s. They are unique within a run and never reused, the
/// protocol asks for nothing more, and an id that matches hotpath's own report
/// makes a disagreement between the two diagnosable.
#[derive(Debug)]
pub struct Stream {
    pid: u32,
    seq: u64,
    elapsed_ns: u64,
    readings: HashMap<u32, Reading>,
}

impl Stream {
    pub fn new(pid: u32) -> Self {
        Stream {
            pid,
            seq: 0,
            elapsed_ns: 0,
            readings: HashMap::new(),
        }
    }

    /// Whether a fresh reading continues the run this stream describes. A
    /// changed pid or an elapsed time that moved backwards both mean the
    /// process was replaced, and differencing across that boundary would
    /// report a new process's whole lifetime as one window.
    pub fn same_run(&self, pid: u32, elapsed_ns: u64) -> bool {
        self.pid == pid && elapsed_ns >= self.elapsed_ns
    }

    /// Folds one report into the stream and returns the frame it implies.
    ///
    /// The first report of a run yields registrations and no events. Its
    /// counters cover everything the process did before the bridge attached,
    /// and charging that to a single window would misreport the rate the
    /// interface sorts by. It is the same fabricated delta a truncated row
    /// would produce, arriving by a different route.
    pub fn absorb(&mut self, report: &JsonFunctionsList) -> Frame {
        let priming = self.readings.is_empty();
        let mut registers = Vec::new();
        let mut events = Vec::new();

        for entry in &report.data {
            let reading = Reading {
                calls: entry.calls,
                total_ns: hotpath::parse_duration(&entry.total).unwrap_or(0),
            };
            let previous = self.readings.insert(entry.id, reading);
            if previous.is_none() {
                registers.push(register_for(entry));
            }
            let Some(previous) = previous else { continue };
            if priming {
                continue;
            }
            let Some(count) = entry.calls.checked_sub(previous.calls).filter(|c| *c > 0) else {
                continue;
            };
            events.push(Event::Calls {
                func: FunctionId(entry.id),
                count,
                ns: window_duration(reading.total_ns.saturating_sub(previous.total_ns), count),
            });
        }

        self.elapsed_ns = report.total_elapsed_ns;
        let frame = Frame {
            seq: self.seq,
            t_ns: report.total_elapsed_ns,
            registers,
            events,
        };
        self.seq += 1;
        frame
    }
}

/// The window's durations, as much as the source knows about them.
///
/// A window short enough that `total` did not move in its fourth significant
/// figure gives an empty distribution. That says the calls happened and their
/// duration is below what the source reports, which is true. Bucketing the
/// zero instead would claim they took no time.
fn window_duration(delta_total_ns: u64, count: u64) -> Dist {
    if delta_total_ns == 0 {
        return Dist::empty();
    }
    let mean_ns = delta_total_ns / count;
    Dist::Buckets {
        b: vec![(bucket_of(mean_ns), count.min(u32::MAX as u64) as u32)],
    }
}

fn register_for(entry: &JsonFunctionEntry) -> Register {
    let (module, name) = match entry.name.split_once("::") {
        Some((crate_name, rest)) => (Some(crate_name.to_string()), rest.to_string()),
        None => (None, entry.name.clone()),
    };
    Register::Function {
        id: FunctionId(entry.id),
        name,
        module,
        location: entry.location.as_ref().map(|l| Location {
            file: l.file.clone(),
            line: l.line,
            column: Some(l.column),
        }),
    }
}

/// What to call the target in the interface. hotpath reports the instrumented
/// entry point, so `lightphotos::main` names the application.
pub fn app_name(caller_name: &str) -> &str {
    caller_name.split("::").next().unwrap_or(caller_name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Value;

    const T0: &str = include_str!("../fixtures/functions_timing-t0.json");
    const T1: &str = include_str!("../fixtures/functions_timing-t1.json");

    fn report(json: &str) -> JsonFunctionsList {
        serde_json::from_str(json).expect("fixture is a hotpath report")
    }

    /// Reparses a fixture through `serde_json::Value` so a test can state the
    /// one thing it changes instead of carrying a second copy of the capture.
    fn edited(json: &str, edit: impl FnOnce(&mut Vec<Value>)) -> JsonFunctionsList {
        let mut value: Value = serde_json::from_str(json).expect("fixture is json");
        let rows = value["data"].as_array_mut().expect("data is an array");
        let mut owned = std::mem::take(rows);
        edit(&mut owned);
        *rows = owned;
        serde_json::from_value(value).expect("edited fixture is still a report")
    }

    fn row_of(rows: &mut [Value], id: u64) -> &mut Value {
        rows.iter_mut()
            .find(|r| r["id"].as_u64() == Some(id))
            .expect("fixture has that id")
    }

    fn calls_of(frame: &Frame) -> Vec<(u32, u64)> {
        let mut seen: Vec<(u32, u64)> = frame
            .events
            .iter()
            .map(|e| match e {
                Event::Calls { func, count, .. } => (func.0, *count),
                other => panic!("the bridge cannot derive {other:?} from hotpath"),
            })
            .collect();
        seen.sort_unstable();
        seen
    }

    fn primed() -> (Stream, Frame) {
        let mut stream = Stream::new(4384);
        let frame = stream.absorb(&report(T0));
        (stream, frame)
    }

    #[test]
    fn the_real_three_second_gap_yields_exactly_the_calls_that_happened() {
        let (mut stream, _) = primed();
        let frame = stream.absorb(&report(T1));
        assert_eq!(calls_of(&frame), vec![(6, 3), (7, 3)]);
    }

    #[test]
    fn the_windows_duration_is_the_mean_the_totals_imply() {
        let (mut stream, _) = primed();
        let frame = stream.absorb(&report(T1));
        // request_working_thumbs: 4.42 ms -> 4.48 ms over 3 calls.
        let expected = Dist::Buckets {
            b: vec![(bucket_of(60_000 / 3), 3)],
        };
        let found = frame
            .events
            .iter()
            .find_map(|e| match e {
                Event::Calls { func, ns, .. } if func.0 == 7 => Some(ns.clone()),
                _ => None,
            })
            .expect("id 7 reported calls");
        assert_eq!(found, expected);
        assert_eq!(found.count(), 3, "the distribution must cover every call");
    }

    #[test]
    fn a_first_report_registers_every_function_and_charges_it_nothing() {
        let (_, frame) = primed();
        assert_eq!(frame.registers.len(), report(T0).data.len());
        assert!(
            frame.events.is_empty(),
            "counters at attach cover the whole process, not this window"
        );
    }

    #[test]
    fn no_event_ever_names_a_function_that_was_not_registered_first() {
        let mut stream = Stream::new(4384);
        let mut known = std::collections::HashSet::new();
        for frame in [stream.absorb(&report(T0)), stream.absorb(&report(T1))] {
            for register in &frame.registers {
                let Register::Function { id, .. } = register else {
                    panic!("the bridge registers functions only")
                };
                assert!(known.insert(id.0), "id {} registered twice", id.0);
            }
            for (id, _) in calls_of(&frame) {
                assert!(
                    known.contains(&id),
                    "id {id} referenced before registration"
                );
            }
        }
    }

    #[test]
    fn a_function_truncated_out_of_the_list_is_not_reported_as_idle_or_as_new() {
        let (mut stream, _) = primed();
        let truncated = edited(T1, |rows| rows.retain(|r| r["id"].as_u64() != Some(7)));
        let frame = stream.absorb(&truncated);
        assert_eq!(
            calls_of(&frame),
            vec![(6, 3)],
            "an absent row must produce no event of any size"
        );

        let returning = edited(T1, |rows| {
            let row = row_of(rows, 7);
            row["calls"] = Value::from(810);
            row["total"] = Value::from("4.50 ms");
        });
        let frame = stream.absorb(&returning);
        assert_eq!(
            calls_of(&frame),
            vec![(7, 5)],
            "a returning function is diffed against its retained reading, not against zero"
        );
        assert!(
            frame.registers.is_empty(),
            "a returning function must not be registered a second time"
        );
    }

    #[test]
    fn a_restart_is_refused_rather_than_differenced_across() {
        let (stream, _) = primed();
        let t0 = report(T0);
        assert!(stream.same_run(4384, t0.total_elapsed_ns));
        assert!(
            !stream.same_run(4384, 1_000_000),
            "an elapsed time that moved backwards is a new process"
        );
        assert!(
            !stream.same_run(9999, t0.total_elapsed_ns + 1),
            "a changed pid is a new process"
        );
    }

    #[test]
    fn a_restarted_target_starts_from_zero_instead_of_spiking() {
        let (_, _) = primed();
        let restarted = edited(T0, |rows| {
            for row in rows.iter_mut() {
                row["calls"] = Value::from(1);
            }
        });
        let mut fresh = Stream::new(9999);
        let frame = fresh.absorb(&restarted);
        assert!(
            frame.events.is_empty(),
            "a new run primes, it does not spike"
        );
        assert_eq!(frame.seq, 0, "a new run restarts the sequence");
        assert_eq!(frame.registers.len(), restarted.data.len());
    }

    #[test]
    fn a_window_too_short_to_move_the_formatted_total_reports_no_duration() {
        let (mut stream, _) = primed();
        let unmoved = edited(T1, |rows| {
            let row = row_of(rows, 7);
            row["total"] = Value::from("4.42 ms");
        });
        let frame = stream.absorb(&unmoved);
        let ns = frame
            .events
            .iter()
            .find_map(|e| match e {
                Event::Calls { func, ns, .. } if func.0 == 7 => Some(ns.clone()),
                _ => None,
            })
            .expect("the calls still happened");
        assert!(
            ns.is_empty(),
            "a duration the source rounded away must not be reported as zero, got {ns:?}"
        );
    }

    #[test]
    fn a_counter_that_moves_backwards_within_a_run_emits_nothing() {
        let (mut stream, _) = primed();
        let backwards = edited(T1, |rows| {
            row_of(rows, 7)["calls"] = Value::from(2);
        });
        let frame = stream.absorb(&backwards);
        assert_eq!(calls_of(&frame), vec![(6, 3)]);
    }

    #[test]
    fn a_registration_carries_the_source_location_the_interface_links_to() {
        let (_, frame) = primed();
        let found = frame
            .registers
            .iter()
            .find(|r| matches!(r, Register::Function { id, .. } if id.0 == 15))
            .expect("id 15 is registered");
        assert_eq!(
            found,
            &Register::Function {
                id: FunctionId(15),
                name: "thumbnail::get_or_make".into(),
                module: Some("lightphotos".into()),
                location: Some(Location {
                    file: "src/thumbnail.rs".into(),
                    line: 407,
                    column: Some(12),
                }),
            }
        );
    }

    #[test]
    fn the_app_is_named_after_the_crate_that_owns_main() {
        assert_eq!(app_name("lightphotos::main"), "lightphotos");
        assert_eq!(app_name("main"), "main");
    }
}
