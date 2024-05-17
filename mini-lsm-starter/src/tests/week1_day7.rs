use tempfile::tempdir;

use crate::block::{prefix_decoding, prefix_encoding};
use crate::key::KeyVec;
use crate::tests::harness::as_bytes;
use crate::{
    block,
    key::{KeySlice, TS_ENABLED},
    table::{bloom::Bloom, FileObject, SsTable, SsTableBuilder},
};

fn key_of(idx: usize) -> Vec<u8> {
    format!("key_{:010}", idx * 5).into_bytes()
}

fn value_of(idx: usize) -> Vec<u8> {
    format!("value_{:010}", idx).into_bytes()
}

fn num_of_keys() -> usize {
    100
}

#[test]
fn test_task1_bloom_filter() {
    let mut key_hashes = Vec::new();
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        key_hashes.push(farmhash::fingerprint32(&key));
    }
    let bits_per_key = Bloom::bloom_bits_per_key(key_hashes.len(), 0.01);
    println!("bits per key: {}", bits_per_key);
    let bloom = Bloom::build_from_key_hashes(&key_hashes, bits_per_key);
    println!("bloom size: {}, k={}", bloom.filter.len(), bloom.k);
    assert!(bloom.k < 30);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        assert!(bloom.may_contain(farmhash::fingerprint32(&key)));
    }
    let mut x = 0;
    let mut cnt = 0;
    for idx in num_of_keys()..(num_of_keys() * 10) {
        let key = key_of(idx);
        if bloom.may_contain(farmhash::fingerprint32(&key)) {
            x += 1;
        }
        cnt += 1;
    }
    assert_ne!(x, cnt, "bloom filter not taking effect?");
    assert_ne!(x, 0, "bloom filter not taking effect?");
}

#[test]
fn test_task2_sst_decode() {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    let sst = builder.build_for_test(&path).unwrap();
    let sst2 = SsTable::open(0, None, FileObject::open(&path).unwrap()).unwrap();
    let bloom_1 = sst.bloom.as_ref().unwrap();
    let bloom_2 = sst2.bloom.as_ref().unwrap();
    assert_eq!(bloom_1.k, bloom_2.k);
    assert_eq!(bloom_1.filter, bloom_2.filter);
}

#[test]
fn test_task3_block_key_compression() {
    let mut builder = SsTableBuilder::new(128);
    for idx in 0..num_of_keys() {
        let key = key_of(idx);
        let value = value_of(idx);
        builder.add(KeySlice::for_testing_from_slice_no_ts(&key[..]), &value[..]);
    }
    let dir = tempdir().unwrap();
    let path = dir.path().join("1.sst");
    let sst = builder.build_for_test(path).unwrap();
    if TS_ENABLED {
        assert!(
            sst.block_meta.len() <= 34,
            "you have {} blocks, expect 34",
            sst.block_meta.len()
        );
    } else {
        assert!(
            sst.block_meta.len() <= 25,
            "you have {} blocks, expect 25",
            sst.block_meta.len()
        );
    }
}

#[test]
fn test_task3_prefix_key() {
    let first_key = KeyVec::from_vec(key_of(5));
    let key = KeyVec::from_vec(key_of(10));
    let raw = prefix_encoding(&first_key, &key);
    let (key2, _) = prefix_decoding(&first_key, raw.as_slice());
    println!(
        "{:?}, {:?}",
        as_bytes(key.for_testing_key_ref()),
        as_bytes(key2.for_testing_key_ref())
    );
    assert_eq!(key, key2);
}
