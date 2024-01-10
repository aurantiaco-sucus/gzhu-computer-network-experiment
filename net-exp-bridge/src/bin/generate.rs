use std::collections::HashSet;
use std::fs::File;
use rand::prelude::*;
use net_exp_bridge::{Address, Frame, FrameData, Segment};
use std::io::{BufWriter, Write};
use log::info;

/// Count of valid addresses
const VALID_ADDR_CNT: usize = 5000;
/// Count of invalid addresses
const INVALID_ADDR_CNT: usize = 100;
/// Count of segments
const SEG_CNT: usize = 100;
/// Count of valid frames
const VALID_FRAME_CNT: usize = 1000_0000;
/// Count of invalid frames
const INVALID_FRAME_CNT: usize = 10_0000;

/// Generate random byte array of specified size with `fastrand` API.
fn gen_byte_arr<const N: usize>() -> [u8; N] {
    let mut data = [0u8; N];
    data.iter_mut().for_each(|x| *x = fastrand::u8(..));
    data
}

/// Generate a physical address.
fn gen_addr() -> Address {
    Address { data: gen_byte_arr() }
}

/// Generate a pool of physical addresses, unique.
fn gen_addr_pool(count: usize) -> HashSet<Address> {
    let mut unique_set: HashSet<Address> = HashSet::with_capacity(count);
    while unique_set.len() < count {
        unique_set.insert(gen_addr());
    }
    unique_set
}

/// Generate a pool of invalid addresses, unique and not clashing with valid ones.
fn gen_invalid_addr_pool(addr_pool: &HashSet<Address>, count: usize) -> HashSet<Address> {
    let mut unique_set: HashSet<Address> = HashSet::with_capacity(count);
    while unique_set.len() < count {
        let addr = gen_addr();
        if !addr_pool.contains(&addr) {
            unique_set.insert(addr);
        }
    }
    unique_set
}

/// Generate a segment.
fn gen_seg() -> Segment {
    Segment { data: gen_byte_arr() }
}

/// Generate a pool of segments.
fn gen_seg_pool(count: usize) -> HashSet<Segment> {
    let mut unique_set: HashSet<Segment> = HashSet::with_capacity(count);
    while unique_set.len() < count {
        unique_set.insert(gen_seg());
    }
    unique_set
}

/// Generate frame data.
fn gen_data() -> FrameData {
    gen_byte_arr()
}

/// Generate frame with specified pools for source and destination addresses.
fn gen_frame(src_pool: &[Address], src_seg_pool: &[Segment], dst_pool: &[Address]) -> Frame {
    let src = src_pool[fastrand::usize(0..src_pool.len())];
    let src_seg = src_seg_pool[fastrand::usize(0..src_seg_pool.len())];
    let mut dst = src;
    while dst == src {
        dst = dst_pool[fastrand::usize(0..dst_pool.len())];
    }
    let data = gen_data();
    Frame { src, src_seg, dst, data }
}

/// Generate a sequence of frames with `gen_frame` function.
fn gen_frame_seq(src_pool: &[Address], src_seg_pool: &[Segment], dst_pool: &[Address], count: usize) -> Vec<Frame> {
    let mut seq = Vec::with_capacity(count);
    for _ in 0..count {
        seq.push(gen_frame(src_pool, src_seg_pool, dst_pool));
    }
    seq
}

/// Generate a mapping from address to segment from their pools.
fn gen_addr_seg(addr_pool: Vec<Address>, seg_pool: &[Segment]) -> Vec<(Address, Segment)> {
    let mut seq = Vec::with_capacity(addr_pool.len() * seg_pool.len());
    let least = addr_pool.len() / seg_pool.len();
    // assign segment for addresses
    for (i, seg) in seg_pool.iter().enumerate() {
        let begin = i * least;
        for j in 0..least {
            seq.push((addr_pool[begin + j], *seg));
        }
    }
    // treat remaining ones
    if seq.len() < addr_pool.len() {
        let begin = seq.len();
        for i in begin..addr_pool.len() {
            seq.push((addr_pool[i], seg_pool[fastrand::usize(0..seg_pool.len())]));
        }
    }
    seq
}

/// Serialize data for use with simulation binary & human analysis.
fn serialize(addr_seg_seq: &[(Address, Segment)], inv_addr_pool: &[Address], frame_seq: &[Frame]) {
    // encode binary format for use with simulation
    let addr_seg_rmp = File::create("addr_seg.rmp").unwrap();
    let inv_addr_rmp = File::create("inv_addr.rmp").unwrap();
    let frame_rmp = File::create("frame.rmp").unwrap();
    rmp_serde::encode::write(&mut BufWriter::new(addr_seg_rmp), addr_seg_seq).unwrap();
    rmp_serde::encode::write(&mut BufWriter::new(inv_addr_rmp), inv_addr_pool).unwrap();
    rmp_serde::encode::write(&mut BufWriter::new(frame_rmp), frame_seq).unwrap();

    // encode text for human-based analysis
    let addr_seg_file = File::create("addr_seg.txt").unwrap();
    let inv_addr_file = File::create("inv_addr.txt").unwrap();
    let mut addr_seg_bw = BufWriter::new(addr_seg_file);
    let mut inv_addr_bw = BufWriter::new(inv_addr_file);
    for (addr, seg) in addr_seg_seq {
        writeln!(addr_seg_bw, "{} {}", addr, seg).unwrap();
    }
    for addr in inv_addr_pool {
        writeln!(inv_addr_bw, "{}", addr).unwrap();
    }
}


fn main() {
    env_logger::init();

    // create pools
    info!("Address pool...");
    let addr_pool = gen_addr_pool(VALID_ADDR_CNT);
    info!("Invalid address pool...");
    let inv_addr_pool = gen_addr_pool(INVALID_ADDR_CNT);
    info!("Segment pool...");
    let seg_pool = gen_seg_pool(SEG_CNT);

    let addr_pool = addr_pool.into_iter().collect::<Vec<_>>();
    let inv_addr_pool = inv_addr_pool.into_iter().collect::<Vec<_>>();
    let seg_pool = seg_pool.into_iter().collect::<Vec<_>>();

    // fabricate frames
    info!("Frame sequence...");
    let frame_seq = {
        let mut frame_seq = gen_frame_seq(
            &addr_pool, &seg_pool, &addr_pool, VALID_FRAME_CNT);
        let inv_frame_seq = gen_frame_seq(
            &addr_pool, &seg_pool, &inv_addr_pool, INVALID_FRAME_CNT);
        frame_seq.extend_from_slice(&inv_frame_seq);
        frame_seq.shuffle(&mut thread_rng());
        frame_seq
    };

    // generate segment mapping
    let addr_seg_seq = gen_addr_seg(addr_pool, &seg_pool);
    info!("Serialization...");
    serialize(&addr_seg_seq, &inv_addr_pool, &frame_seq);
}