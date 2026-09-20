#![cfg(feature = "enabled")]

use demo_app::{build_tags, build_thumbnails, Tag, Thumbnail};
use lightwatch::testing;

const PIXEL_BYTES: usize = 64 * 1024;

#[test]
fn a_heap_owning_type_is_sized_by_measured_not_by_size_of() {
    let thumbnails = build_thumbnails(8, PIXEL_BYTES);

    let reported = testing::bytes("Thumbnail");
    let truth = 8 * (PIXEL_BYTES + size_of::<Thumbnail>()) as u64;
    let by_size_of = 8 * size_of::<Thumbnail>() as u64;

    assert!(
        reported.abs_diff(truth) * 100 <= truth * 5,
        "{reported} bytes is further than 5% from the real {truth}"
    );
    assert!(
        reported > by_size_of * 100,
        "{reported} bytes is close to the useless size_of total of {by_size_of}; \
         the generated Measured impl is being used instead of the hand-written one"
    );

    drop(thumbnails);
    assert_eq!(testing::bytes("Thumbnail"), 0);
}

#[test]
fn a_type_that_owns_nothing_is_sized_exactly() {
    let tags = build_tags(4);

    // A Tag is a handful of bytes, below the threshold where the protocol's
    // buckets stop being one value each, so this is not an estimate.
    assert!(size_of::<Tag>() < 32);
    assert_eq!(testing::bytes("Tag"), 4 * size_of::<Tag>() as u64);
    assert_eq!(testing::live("Tag"), 4);

    drop(tags);
    assert_eq!(testing::live("Tag"), 0);
}
