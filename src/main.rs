extern crate kv_service;

use std::env;
use std::thread;
use std::time::Duration;
use kv_service::kv::server;
use kv_service::raft::rpc::Client;
use kv_service::kv::client;

fn main() {
	let args: Vec<String> = env::args().collect();

	let server_num = args[1].parse::<u32>().unwrap();
	let cur_id = args[2].parse::<i32>().unwrap();

    let base_port = 8810;
    let mut addrs = Vec::new();
    for i in 0..server_num {
        addrs.push(format!("127.0.0.1:{}", base_port+i));
    }

    let mut clients = Vec::new();
    if cur_id != server_num as i32 {
        server::KVServer::new(cur_id, &addrs);
    } else {
        // client
        for i in 0..server_num {
            clients.push(Client{end_name: String::from(""), server_addr: addrs[i as usize].clone()});
        }
        let mut clerk = client::Clerk::new(&clients, 0);

        for i in 0..500 {
            clerk.put(&String::from(format!("key {}",i)), &String::from(format!("value {}",i)));
            let v = clerk.get(&String::from(format!("key {}",i)));
            println!("get {}", v);
        }

        for i in (0..500).rev() {
            let v = clerk.get(&String::from(format!("key {}",i)));
            println!("get {}", v);
        }
    }

    thread::sleep(Duration::from_secs(6000));
}