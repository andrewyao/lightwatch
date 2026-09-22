//! Alone in its process: it asserts on the whole footprint of a type, which
//! another test building the same type in parallel would move.
#![cfg(feature = "enabled")]

use demo_app::build_thumbnails;
use lightwatch::testing;

const PIXEL_BYTES: usize = 64 * 1024;

#[test]
fn replacing_one_instance_with_another_leaves_the_footprint_where_it_was() {
    let mut first = build_thumbnails(1, PIXEL_BYTES);
    let one_alive = testing::bytes("Thumbnail");
    assert_eq!(testing::live("Thumbnail"), 1);

    // The shape a cache swap takes: the replacement is built while the old
    // value is still held, then the old value drops.
    let second = build_thumbnails(1, PIXEL_BYTES);
    assert_eq!(testing::live("Thumbnail"), 2, "both are alive between the build and the drop");
    assert_eq!(testing::bytes("Thumbnail"), one_alive * 2);

    first.clear();
    assert_eq!(testing::live("Thumbnail"), 1, "the replaced one is gone");
    assert_eq!(
        testing::bytes("Thumbnail"),
        one_alive,
        "one image in, one image out, so the footprint is back where it started"
    );

    drop(second);
    assert_eq!(testing::bytes("Thumbnail"), 0);
}
