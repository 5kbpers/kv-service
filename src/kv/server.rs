use std::time::Duration;
use std::collections::HashMap;
use std::thread;
use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, SyncSender, Receiver, RecvTimeoutError};
use super::super::raft::{Raft, ApplyMsg};
use super::super::raft::rpc::{self, Client};
use super::common::*;
use bincode::{serialize, deserialize};

const START_TIMEOUT_INTERVAL: u64 = 5000; // ms
const CALLBACK_NUMS: usize = 4;

struct NotifyArgs {
    term: u64,
    value: String,
    err: RespErr,
}

pub struct KVServer {
    me: i32,
    rf: Arc<Mutex<Raft>>,
    maxraftstate: u64,

    data: HashMap<String, String>,
    cache: HashMap<u64, u64>,
    notify_ch_map: HashMap<usize, SyncSender<NotifyArgs>>,
}

impl KVServer {
    pub fn new(
        id: i32,
        addrs: &Vec<String>,
        maxraftstate: u64,
        ) -> Client {
        let (s, r) = mpsc::sync_channel(1000);
        let (rf, client, reply_sender, req_recv)= Raft::new(id, addrs, &s);
        let kv = KVServer {
            me: id,
            rf,
            maxraftstate,
            data: HashMap::new(),
            cache: HashMap::new(),
            notify_ch_map: HashMap::new(),
        };
        let kv = Arc::new(Mutex::new(kv));
        Self::register_callback(&kv, reply_sender, req_recv);
        thread::spawn(move || { Self::run(kv, r); });
        client
    }

    pub fn get(mu: Arc<Mutex<KVServer>>, args: &ReqArgs) -> GetReply {
        let args = serialize(args).unwrap();
        let (err, value) = Self::start(mu, &args);
        GetReply{err, value}
    }

    pub fn put_append(mu: Arc<Mutex<KVServer>>, args: &ReqArgs) -> PutAppendReply {
        let args = serialize(args).unwrap();
        let (err, _) = Self::start(mu, &args);
        PutAppendReply{err: err}
    }

    fn notify_if_present(&mut self, index: usize, reply: NotifyArgs) {
        if let Some(sch) = self.notify_ch_map.get(&index) {
            sch.send(reply);
        }
        self.notify_ch_map.remove(&index);
    }

    fn start(mu: Arc<Mutex<KVServer>>, command: &Vec<u8>) -> (RespErr, String) {
        let notify_ch: Receiver<NotifyArgs>;
        let index;
        let term;
        {
            let mut kv = mu.lock().unwrap();
            let (i, t, ok) = Raft::start(kv.rf.clone(), command);
            index = i;
            term = t;
            if ok == false {
                return (RespErr::ErrWrongLeader, String::from(""));
            }
            let (sh, rh) = mpsc::sync_channel(0);
            notify_ch = rh;
            kv.notify_ch_map.insert(index, sh);
        }
        let d = Duration::from_millis(START_TIMEOUT_INTERVAL);
        match notify_ch.recv_timeout(d) {
            Ok(result) => {
                if result.term != term {
                    return (RespErr::ErrWrongLeader,  String::from(""));
                }
                return (result.err, result.value);
            }
            Err(RecvTimeoutError::Timeout) | Err(RecvTimeoutError::Disconnected) => {
                println!("---------------------start timeout---------------------");
                let mut kv = mu.lock().unwrap();
                kv.notify_ch_map.remove(&index);
                return (RespErr::ErrWrongLeader,  String::from(""));
            }
        }
    }

    fn apply(&mut self, msg :&ApplyMsg) {
        println!("---------------apply");
        let mut result = NotifyArgs{
            term: msg.term,
            value: String::from(""),
            err: RespErr::OK
        };
        let args: ReqArgs = deserialize(&msg.command).unwrap();
        if args.request_type == 0 {
            let value = self.data.get(&args.key);
            match value {
                Some(v) => result.value = v.clone(),
                None => result.value = String::from(""),
            }
        } else if args.request_type == 1 {
            let seq = self.cache.get(&args.cliend_id);
            let mut flag = true;
            match seq {
                Some(n) => {
                    if *n >= args.request_seq {
                        flag = false;
                    }
                },
                None => (),
            }
            if flag {
                if args.op == "Put" {
                    self.data.insert(args.key, args.value);
                } else {
                    let value = self.data.get(&args.key);
                    match value {
                        Some(v) => self.data.insert(args.key, format!("{}{}", v, args.value)),
                        None => self.data.insert(args.key, args.value),
                    };
                }
            }
        } else {
            result.err = RespErr::ErrWrongLeader;
        }
        self.notify_if_present(msg.index, result);
    }

    fn run(mu: Arc<Mutex<KVServer>>, apply_ch: Receiver<ApplyMsg>) {
        loop {
            let msg = apply_ch.recv();
//            println!("--------receive message");
            match msg {
                Ok(m) => {
                    let mut kv = mu.lock().unwrap();
                    if m.valid {
                        kv.apply(&m);
                    }
                },
                Err(_) => continue,
            }
        }
    }

    fn register_callback(
        kv: &Arc<Mutex<KVServer>>,
        mut reply_sender: Vec<SyncSender<(Vec<u8>, bool)>>,
        mut req_recv: Vec<Receiver<Vec<u8>>>
    ) {
        let kv1 = kv.clone();
        let get_req = req_recv.remove(0);
        let get_reply = reply_sender.remove(0);
        thread::spawn(move || { //RequestVote
            loop {
                let args = get_req.recv().unwrap();

                let req : ReqArgs = deserialize(&args[..]).unwrap();
                let reply = Self::get(kv1.clone(), &req);
                let reply = serialize(&reply).unwrap();
                get_reply.send((reply, true)).unwrap();
            }
        });

        let kv2 = kv.clone();
        let put_req = req_recv.remove(0);
        let put_reply = reply_sender.remove(0);
        thread::spawn(move || { //RequestVote
            loop {
                let args = put_req.recv().unwrap();

                let req : ReqArgs = deserialize(&args[..]).unwrap();
                let reply = Self::put_append(kv2.clone(), &req);
                let reply = serialize(&reply).unwrap();
                put_reply.send((reply, true)).unwrap();
            }
        });
    }
}
