use std::env::args;
use std::fs::File;
use std::io::{BufReader, Read};
use std::time::Instant;

fn main() {
    let path = args().nth(1).unwrap();
    let file = File::open(path).unwrap();
    let mut file = BufReader::new(file);
    let begin = Instant::now();
    let mut buf = [0; 2];
    let mut sum = 0_u16;
    while let Ok(n) = file.read(&mut buf) {
        if n == 0 { break; }
        if n == 1 { buf[1] = 0; }
        let val = u16::from_be_bytes(buf);
        let (mut ns, of) = sum.overflowing_add(val);
        if of { ns += 1; }
        sum = ns;
    }
    let time = begin.elapsed();
    println!("Elapse: {time:?}");
    println!("Checksum: {sum:x}");
}
