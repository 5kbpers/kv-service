use std::thread;
use std::time::Duration;
use super::common::*;
use super::super::raft::rpc::Client;
use bincode::{serialize, deserialize};

pub struct Clerk {
    servers: Vec<Client>,
    client_id: u64,
    request_seq: u64,
    leader_id: i32,
}

impl Clerk {
    pub fn new(servers: &Vec<Client>, client_id: u64) -> Clerk {
        Clerk {
            servers: servers.clone(),
            client_id,
            request_seq: 0,
            leader_id: 0,
        }
    }

    pub fn get(&mut self, key: &String) -> String {
        let args = ReqArgs{
            request_type: 0,
            request_seq: self.request_seq,
            cliend_id: self.client_id,
            key: key.clone(),
            value: String::new(),
            op: String::new(),
        };
        let req = serialize(&args).unwrap();
        loop {
            let (reply, success) = self.servers[self.leader_id as usize].Call(
                String::from("KV.Get"),
                req.clone(),
                );
            if success {
                let reply: GetReply = deserialize(&reply).unwrap();
                match reply.err {
                    RespErr::OK => return reply.value,
                    RespErr::ErrWrongLeader => (),
                }
            }
            self.leader_id = (self.leader_id + 1) % (self.servers.len() as i32);
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn put(&mut self, key: &String, value: &String) {
        let op = String::from("Put");
        self.put_append(key, value, &op);
    }

    pub fn append(&mut self, key: &String, value: &String) {
        let op = String::from("Append");
        self.put_append(key, value, &op);
    }

    fn put_append(&mut self, key: &String, value: &String, op: &String) {
        self.request_seq += 1;
        let args = ReqArgs{
            request_type: 0,
            request_seq: self.request_seq,
            cliend_id: self.client_id,
            key: key.clone(),
            value: value.clone(),
            op: op.clone(),
        };
        let req = serialize(&args).unwrap();
        loop {
            let (reply, success) = self.servers[self.leader_id as usize].Call(
                String::from("KV.PutAppend"),
                req.clone(),
                );
            if success {
                let reply: PutAppendReply = deserialize(&reply).unwrap();
                match reply.err {
                    RespErr::OK => return,
                    RespErr::ErrWrongLeader => (),
                }
            }
            self.leader_id = (self.leader_id + 1) % (self.servers.len() as i32);
            thread::sleep(Duration::from_millis(100));
        }
    }
}
