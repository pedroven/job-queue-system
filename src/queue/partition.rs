/// FNV-1a 32-bit. We need a *stable* hash — `std::hash` uses a randomized
/// hasher per process, which would route the same `job.id` to different
/// partitions on different hosts. FNV-1a is small, fast, and dependency-free,
/// and good enough for distributing UUID v4 ids (which are already
/// high-entropy).
pub fn stable_hash(s: &str) -> u32 {
    let mut h: u32 = 0x811c9dc5;
    for b in s.as_bytes() {
        h ^= *b as u32;
        h = h.wrapping_mul(0x01000193);
    }
    h
}

pub fn partition_for(job_id: &str, partition_count: u32) -> u32 {
    assert!(partition_count > 0, "partition_count must be > 0");
    stable_hash(job_id) % partition_count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_same_input_same_partition() {
        assert_eq!(partition_for("abc", 4), partition_for("abc", 4));
    }

    #[test]
    fn test_distribution_is_roughly_even() {
        // Heuristic: 4 buckets, 4000 uuid-like ids → each bucket within
        // ±40% of the mean (1000). Loose because chi-square would
        // overcomplicate a smoke test, but tight enough to catch a
        // pathological hash (e.g. always returning 0).
        let mut buckets = [0_usize; 4];
        for i in 0..4000 {
            let id = format!("{i:08x}-{i:04x}-{i:04x}-{i:04x}-{i:012x}");
            buckets[partition_for(&id, 4) as usize] += 1;
        }
        for (i, &count) in buckets.iter().enumerate() {
            assert!(
                (600..=1400).contains(&count),
                "bucket {i} skewed: {count} (expected 600..=1400)"
            );
        }
    }

    #[test]
    fn test_partition_count_one_always_zero() {
        for i in 0..100 {
            assert_eq!(partition_for(&format!("id-{i}"), 1), 0);
        }
    }
}
