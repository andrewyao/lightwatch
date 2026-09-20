//! Alone in its process: it asserts on the whole live count for a type, which
//! another test building the same type in parallel would move.
#![cfg(feature = "enabled")]

use demo_app::build_thumbnails;
use lightwatch::testing;

const PIXEL_BYTES: usize = 64 * 1024;

#[test]
fn live_instances_follow_construction_and_drop_back_to_zero() {
    assert_eq!(testing::live("Thumbnail"), 0, "nothing is alive before the first one is built");

    let mut thumbnails = build_thumbnails(10, PIXEL_BYTES);
    assert_eq!(testing::live("Thumbnail"), 10);
    let buckets = testing::size_buckets("Thumbnail");
    assert_eq!(buckets.len(), 1, "ten identical thumbnails land in one bucket, got {buckets:?}");
    assert_eq!(buckets[0].1, 10);

    thumbnails.truncate(5);
    assert_eq!(testing::live("Thumbnail"), 5, "freeing half halves the live count");
    assert_eq!(
        testing::size_buckets("Thumbnail")[0].1,
        5,
        "the drops came out of the bucket their constructions went into"
    );

    drop(thumbnails);
    assert_eq!(testing::live("Thumbnail"), 0);
    assert!(
        testing::size_buckets("Thumbnail").is_empty(),
        "an empty census leaves no bucket standing"
    );
}
