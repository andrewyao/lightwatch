//! What the `enabled` feature costs, measured on the same source both ways.

use demo_app::Thumbnail;
use lightwatch::Census;

#[cfg(not(feature = "enabled"))]
#[test]
fn a_census_is_zero_sized_and_never_dropped_when_the_feature_is_off() {
    assert_eq!(size_of::<Census<Thumbnail>>(), 0);
    assert!(!std::mem::needs_drop::<Census<Thumbnail>>());
    assert_eq!(
        size_of::<Thumbnail>(),
        size_of::<Vec<u8>>(),
        "the injected field must not change what a tracked struct costs"
    );
}

#[cfg(feature = "enabled")]
#[test]
fn a_census_is_the_two_bytes_of_its_bucket_when_the_feature_is_on() {
    assert_eq!(size_of::<Census<Thumbnail>>(), 2);
    assert!(
        size_of::<Thumbnail>() > size_of::<Vec<u8>>(),
        "measuring has to cost the bucket index it stores"
    );
}
