use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::sync::mpsc::{Receiver, Sender};
use std::{fs, thread};
use std::f64::consts::PI;
use std::time::{Duration, Instant};
use log::info;
use serde_pickle::SerOptions;
use net_exp_bridge::{Address, Frame, Segment};

const ELAPSE_SEC: usize = 10;

/// Event that bridge receives.
enum Event {
    /// Incoming request of routing a frame.
    Request(Frame),
    /// Found segment that accept an address.
    Success(Address, Segment),
    /// No segment accepts an address.
    Failure(Address),
    /// Simulation finishing and the bridge should be exiting.
    Shutdown,
}

/// Command that bridge emits.
enum Command {
    /// Broadcast an address to segments
    Broadcast(Address),
    /// Dispatch a frame to a segment
    Dispatch(Frame, Segment),
    /// Discard a frame
    Discard(Frame),
}

/// Waiting list of frames.
struct Holder {
    map: BTreeMap<Address, Vec<Frame>>
}

impl Holder {
    fn new() -> Self {
        Holder { map: BTreeMap::new() }
    }

    /// Check if there exist frames of a specific address.
    fn exist_addr(&self, addr: &Address) -> bool {
        self.map.contains_key(addr)
    }

    /// Hold a frame.
    fn hold(&mut self, frame: Frame) {
        let frames = self.map.entry(frame.dst)
            .or_insert_with(Vec::new);
        frames.push(frame);
    }

    /// Release frames of the same address.
    fn release(&mut self, addr: Address) -> Vec<Frame> {
        self.map.remove(&addr).unwrap_or_default()
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

/// Statistics of bridge
pub enum BridgeStatRecord {
    Broadcast(Frame),
    Dispatch(Frame),
    Discard(Frame),
}

impl BridgeStatRecord {
    pub fn frame(&self) -> &Frame {
        match self {
            BridgeStatRecord::Broadcast(frame) => frame,
            BridgeStatRecord::Dispatch(frame) => frame,
            BridgeStatRecord::Discard(frame) => frame,
        }
    }
}

/// Record of bridge statistics.
pub struct BridgeStat {
    pub records: Vec<BridgeStatRecord>,
    pub times: Vec<Instant>,
    pub init: Instant,
}

impl BridgeStat {
    fn new() -> Self {
        BridgeStat { records: Vec::new(), times: Vec::new(), init: Instant::now() }
    }

    fn broadcast(&mut self, frame: Frame) {
        self.records.push(BridgeStatRecord::Broadcast(frame));
        self.times.push(Instant::now());
    }

    fn dispatch(&mut self, frame: Frame) {
        self.records.push(BridgeStatRecord::Dispatch(frame));
        self.times.push(Instant::now());
    }

    fn discard(&mut self, frame: Frame) {
        self.records.push(BridgeStatRecord::Discard(frame));
        self.times.push(Instant::now());
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    /// Export scatter of different types of activities.
    fn export_activity_scatter(&self) {
        let sc_src = self.records.iter()
            .zip(self.times.iter())
            .map(|(x, y)| (x, y.duration_since(self.init).as_micros()));

        let mut sc_broadcast = Vec::with_capacity(self.records.len());
        let mut sc_dispatch = Vec::with_capacity(self.records.len());
        let mut sc_discard = Vec::with_capacity(self.records.len());

        for (x, y) in sc_src {
            match x {
                BridgeStatRecord::Broadcast(_) => sc_broadcast.push(y as i64),
                BridgeStatRecord::Dispatch(_) => sc_dispatch.push(y as i64),
                BridgeStatRecord::Discard(_) => sc_discard.push(y as i64),
            }
        }

        let mut w_broadcast = BufWriter::new(File::create("sc_broadcast_activity.pkl").unwrap());
        let mut w_dispatch = BufWriter::new(File::create("sc_dispatch_activity.pkl").unwrap());
        let mut w_discard = BufWriter::new(File::create("sc_discard_activity.pkl").unwrap());

        serde_pickle::to_writer(&mut w_broadcast, &sc_broadcast, SerOptions::default()).unwrap();
        serde_pickle::to_writer(&mut w_dispatch, &sc_dispatch, SerOptions::default()).unwrap();
        serde_pickle::to_writer(&mut w_discard, &sc_discard, SerOptions::default()).unwrap();
    }

    /// Export scatter of latencies of frames broadcast.
    fn export_latency_scatter(&self) {
        let mut hold_map = HashMap::<Frame, u128>::new();
        let mut latencies = Vec::with_capacity(self.records.len());
        for (rec, t) in self.records.iter().zip(self.times.iter()) {
            let t = t.duration_since(self.init).as_micros();
            match rec {
                BridgeStatRecord::Broadcast(frame) => {
                    hold_map.insert(frame.clone(), t);
                }
                BridgeStatRecord::Dispatch(frame) | BridgeStatRecord::Discard(frame) => {
                    let begin = if let Some(val) = hold_map.remove(&frame) { val } else {
                        continue
                    };
                    let lat = t - begin;
                    latencies.push(vec![begin as i64, lat as i64]);
                }
            }
        }
        serde_pickle::to_writer(&mut BufWriter::new(File::create("sc_latency.pkl").unwrap()),
                                &latencies, SerOptions::default()).unwrap();
    }
}

/// Statistics of pending frames of bridge.
pub struct BridgePendingStat {
    pub records: Vec<usize>,
    pub times: Vec<Instant>,
    pub init: Instant,
}

impl BridgePendingStat {
    fn new() -> Self {
        BridgePendingStat { records: Vec::new(), times: Vec::new(), init: Instant::now() }
    }

    fn rec(&mut self, count: usize) {
        self.records.push(count);
        self.times.push(Instant::now());
    }

    fn len(&self) -> usize {
        self.records.len()
    }

    /// Export scatter of congestion, the changing pressure of waiting list.
    fn export_congestion_scatter(&self) {
        let sc_congestion = self.records.iter()
            .zip(self.times.iter())
            .map(|(x, y)| (x, y.duration_since(self.init).as_micros()))
            .map(|(x, y)| vec![y as i64, *x as i64])
            .collect::<Vec<_>>();
        serde_pickle::to_writer(&mut BufWriter::new(File::create("sc_congestion.pkl").unwrap()),
                                &sc_congestion, SerOptions::default()).unwrap();
    }
}

/// Launch network bridge
fn bridge(tc: Sender<Command>, re: Receiver<Event>) {
    info!(target: "bridge", "Bridge started.");
    let mut mapping = BTreeMap::new();
    let mut pending = Holder::new();
    let mut stat = BridgeStat::new();
    let mut pending_stat = BridgePendingStat::new();
    let mut req_cnt = 0;
    let mut b_cnt = 0;
    let mut dp_cnt = 0;
    let mut dc_cnt = 0;
    let mut last_t = Instant::now();
    while let Ok(event) = re.recv() { // receive an event
        match event {
            Event::Request(frame) => {
                if mapping.get(&frame.src).is_none() {
                    // correlate the source address with incoming segment
                    mapping.insert(frame.src, frame.src_seg);
                }
                if let Some(segment) = mapping.get(&frame.dst) {
                    // dispatch if source found in mapping
                    stat.dispatch(frame.clone());
                    tc.send(Command::Dispatch(frame, *segment)).unwrap();
                    req_cnt += 1;
                    dp_cnt += 1;
                } else if !pending.exist_addr(&frame.dst) {
                    // broadcast if no frames of same source are waiting
                    stat.broadcast(frame.clone());
                    tc.send(Command::Broadcast(frame.dst)).unwrap(); // <- actual command
                    pending_stat.rec(pending.len());
                    pending.hold(frame);
                    b_cnt += 1;
                } else {
                    stat.broadcast(frame.clone());
                    pending_stat.rec(pending.len());
                    pending.hold(frame);
                }
            }
            Event::Success(address, segment) => {
                // update the mapping
                mapping.insert(address, segment);
                for frame in pending.release(address) {
                    // dispatch all frames with the same segment
                    stat.dispatch(frame.clone());
                    tc.send(Command::Dispatch(frame, segment)).unwrap();
                    dp_cnt += 1;
                }
                pending_stat.rec(pending.len());
            }
            Event::Failure(address) => {
                for frame in pending.release(address) {
                    // discard them all
                    stat.discard(frame.clone());
                    tc.send(Command::Discard(frame)).unwrap();
                    dc_cnt += 1;
                }
                pending_stat.rec(pending.len());
            }
            Event::Shutdown => {
                info!(target: "bridge", "Received shutdown signal.");
                // export statistics
                stat.export_activity_scatter();
                stat.export_latency_scatter();
                pending_stat.export_congestion_scatter();
                break;
            }
        }
        if last_t.elapsed() > Duration::from_millis(50) {
            info!(target: "bridge", "Received {} requests. Done {} broadcasts, {} dispatches and {} discards.",
                    req_cnt, b_cnt, dp_cnt, dc_cnt);
            req_cnt = 0;
            b_cnt = 0;
            dp_cnt = 0;
            dc_cnt = 0;
            last_t = Instant::now();
        }
    }
    info!(target: "bridge", "Bridge exiting.");
}

/// Cumulative distribution function of the distribution of "half circle".
///
/// Its PDF (Probability Density Function)'s graph will look like one top half of a circle fitted
/// in the square of x from 0 to 1 and y from 0 to 1.
fn half_circle_dist_cdf(x: f64) -> f64 {
    let x = x * PI - PI / 2.0;
    (x.sin() + 1.0) / 2.0
}

/// Distribute the frames per milliseconds in specified duration with a distribution function.
fn distribute(frame_seq: Vec<Frame>, dur_sec: usize, dist: fn(f64) -> f64) -> Vec<Vec<Frame>> {
    let mut buckets = vec![Vec::new(); dur_sec * 1000];
    let mut last_pos = 0;
    let dur = dur_sec * 1000;
    for (i, vec) in buckets.iter_mut().enumerate() {
        let pos = (dist(i as f64 / dur as f64) * frame_seq.len() as f64) as usize;
        vec.extend_from_slice(&frame_seq[last_pos..pos]);
        last_pos = pos;
    }
    // collect remaining bits if any
    if last_pos < frame_seq.len() {
        buckets.last_mut().unwrap().extend_from_slice(&frame_seq[last_pos..]);
    }
    buckets
}

/// Orchestration service that send frames to the bridge with distributed frame sequence.
fn orchestrator(frame_seq: Vec<Frame>, te: Sender<Event>) {
    info!(target: "orchestrator", "Orchestrator started.");
    let frame_seq = distribute(frame_seq, ELAPSE_SEC, half_circle_dist_cdf);
    let begin = Instant::now();
    let mut last = 0;
    let mut last_t = Instant::now();
    let mut count = 0;
    loop {
        let now = Instant::now();
        let dur = now.duration_since(begin);
        let cur = dur.as_secs() * 1000 + dur.subsec_millis() as u64;
        if cur >= frame_seq.len() as u64 {
            for buckets in frame_seq[last..].iter() {
                for frame in buckets {
                    te.send(Event::Request(frame.clone())).unwrap();
                }
            }
            break;
        }
        if cur > last as u64 {
            for buckets in frame_seq[last..cur as usize].iter() {
                for frame in buckets {
                    te.send(Event::Request(frame.clone())).unwrap();
                    count += 1;
                }
            }
            last = cur as usize;
        }
        if now.duration_since(last_t) > Duration::from_millis(250) {
            info!(target: "orchestrator", "Sent {} frames.", count);
            count = 0;
            last_t = now;
        }
        thread::sleep(Duration::from_millis(1));
    }
    info!(target: "orchestrator", "Orchestrator exiting.");
}

/// Meter to count facility statistics within some time.
struct FacilityMeter {
    s_cnt: usize,
    f_cnt: usize,
    dp_cnt: usize,
    dc_cnt: usize,
}

impl FacilityMeter {
    fn new() -> Self {
        FacilityMeter { s_cnt: 0, f_cnt: 0, dp_cnt: 0, dc_cnt: 0 }
    }

    fn inc_success(&mut self) {
        self.s_cnt += 1;
    }

    fn inc_failure(&mut self) {
        self.f_cnt += 1;
    }

    fn inc_dispatch(&mut self) {
        self.dp_cnt += 1;
    }

    fn inc_discard(&mut self) {
        self.dc_cnt += 1;
    }

    fn report(&mut self) {
        info!(target: "facility", "Handled {} successes, {} failures, {} dispatches and {} discards.",
            self.s_cnt, self.f_cnt, self.dp_cnt, self.dc_cnt);
        self.s_cnt = 0;
        self.f_cnt = 0;
        self.dp_cnt = 0;
        self.dc_cnt = 0;
    }
}

/// Facilitation service that handle commands from the bridge.
fn facility(count: usize, mapping: BTreeMap<Address, Segment>, te: Sender<Event>, rc: Receiver<Command>) {
    info!(target: "facility", "Facility started.");
    let mut cur_n = 0;
    let mut meter = FacilityMeter::new();
    let mut last_t = Instant::now();
    while let Ok(command) = rc.recv() {
        match command {
            Command::Broadcast(addr) => {
                if let Some(segment) = mapping.get(&addr) {
                    te.send(Event::Success(addr, *segment)).unwrap();
                    meter.inc_success();
                } else {
                    te.send(Event::Failure(addr)).unwrap();
                    meter.inc_failure();
                }
            }
            Command::Dispatch(_, _) => {
                meter.inc_dispatch();
                cur_n += 1;
            }
            Command::Discard(_) => {
                meter.inc_discard();
                cur_n += 1;
            }
        }
        if last_t.elapsed() > Duration::from_millis(250) {
            meter.report();
            last_t = Instant::now();
        }
        if cur_n == count {
            te.send(Event::Shutdown).unwrap();
            break;
        }
    }
    info!(target: "facility", "Facility exiting.");
}

/// Load segment mapping from disk.
fn load_mapping() -> BTreeMap<Address, Segment> {
    let addr_seg = BufReader::new(File::open("addr_seg.rmp").unwrap());
    let addr_seg: Vec<(Address, Segment)> = rmp_serde::from_read(addr_seg).unwrap();
    BTreeMap::from_iter(addr_seg)
}

/// Load generated frames from disk.
fn load_frames() -> Vec<Frame> {
    let frame = BufReader::new(File::open("frame.rmp").unwrap());
    rmp_serde::from_read(frame).unwrap()
}

fn main() {
    env_logger::init();
    let (tc, rc) = std::sync::mpsc::channel();
    let (te, re) = std::sync::mpsc::channel();
    let frames = load_frames();

    let facility = {
        let mapping = load_mapping();
        let te = te.clone();
        let len = frames.len();
        thread::spawn(move || facility(len, mapping, te, rc))
    };

    let bridge = {
        let tc = tc.clone();
        thread::spawn(move || bridge(tc, re))
    };

    let orchestrator = {
        let te = te.clone();
        thread::spawn(move || orchestrator(frames, te))
    };

    orchestrator.join().unwrap();
    facility.join().unwrap();
    bridge.join().unwrap();
}