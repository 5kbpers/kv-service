extern crate kv_service;

use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use std::sync::mpsc::{sync_channel};
use kv_service::raft::Raft;

fn main() {
	let args: Vec<String> = env::args().collect();

	let server_num = args[1].parse::<u32>().unwrap();
	let cur_id = args[2].parse::<i32>().unwrap();

    let mut base_port = 8810;
    let mut addrs = Vec::new();
    for i in (0..server_num) {
        addrs.push(format!("127.0.0.1:{}", base_port));
        base_port += 1;
    }

    let (sx, rx) = sync_channel(1);
    let raft = Raft::new(cur_id, &addrs, &sx);
    thread::sleep(Duration::from_secs(60));
}