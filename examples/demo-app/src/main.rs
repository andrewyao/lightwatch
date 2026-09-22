//! Drives the demo workload long enough for a daemon to see it change.
//!
//! Builds a known number of objects, frees half of them, holds the rest, then
//! frees those too, so a census watcher sees the live count rise, halve, and
//! reach zero rather than one steady number.
//!
//! Two threads run the workload, because a call tree is process-wide: the two
//! of them meet on the same contexts, and their self time merges there. A
//! single-threaded demo would never exercise that, and would let an interface
//! get away with normalising a flame width against wall-clock time.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use demo_app::{build_tags, build_thumbnails, import, one_round};

const TAGS: usize = 200;
const THUMBNAILS: usize = 64;
const PIXEL_BYTES: usize = 64 * 1024;

fn main() {
    lightwatch::start_named("demo-app");

    let rounds: u32 = std::env::var("DEMO_ROUNDS")
        .ok()
        .and_then(|raw| raw.parse().ok())
        .unwrap_or(20);
    let window = Duration::from_millis(100);

    // A second thread on the same contexts, at its own pace, so the feed is
    // not a tidy single-threaded metronome.
    let stop = Arc::new(AtomicBool::new(false));
    let worker = std::thread::spawn({
        let stop = Arc::clone(&stop);
        move || {
            while !stop.load(Ordering::Relaxed) {
                import::run(2, 192);
                std::thread::sleep(Duration::from_millis(30));
            }
        }
    });

    let mut tags = build_tags(TAGS);
    let mut thumbnails = build_thumbnails(THUMBNAILS, PIXEL_BYTES);
    println!("built {} tags and {} thumbnails", tags.len(), thumbnails.len());

    for round in 0..rounds {
        let total = one_round();
        if round == rounds / 2 {
            tags.truncate(TAGS / 2);
            thumbnails.truncate(THUMBNAILS / 2);
            println!("freed half: {} tags, {} thumbnails", tags.len(), thumbnails.len());
        }
        std::thread::sleep(window / 2);
        if round == 0 {
            println!("first round total {total}");
        }
    }

    stop.store(true, Ordering::Relaxed);
    worker.join().expect("the second worker should not panic");

    drop(tags);
    drop(thumbnails);
    println!("freed the rest");
    // Long enough for a window to close on an empty census, so a watcher sees
    // the live count reach zero instead of just stopping.
    std::thread::sleep(window * 3);
}
