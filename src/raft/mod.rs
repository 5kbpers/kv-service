use std::sync::{Arc, Mutex};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::thread;
use std::time::Duration;

use bincode::{deserialize, serialize};
use rand::Rng;

use self::rpc::Client;
use self::State::{Candidate, Follower, Leader};
use self::storage::Storage;

mod storage;
mod rpc;
mod util;

const HEARBEAT_INTERVAL: u64 = 50;
//const ELECTION_TIMEOUT:u64 = 1000;
const MIN_TIMEOUT: u64 = 300;
const MAX_TIMEOUT: u64 = 600;

pub enum State {
    Follower,
    Candidate,
    Leader,
}

enum Message {
    Shutdown,
    Reset,
    VoteReply(RequestVoteReply),
    HeartbeatReply(AppendEntriesReply),
}

#[derive(Serialize, Deserialize, PartialEq, Clone, Debug)]
pub struct LogEntry {
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
}

pub struct ApplyMsg {
    pub valid: bool,
    pub index: u64,
    pub term: u64,
    pub command: Vec<u8>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: i32,
    pub last_log_index: usize,
    pub last_log_term: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub confilct_index: u64,
    pub confilct_term: u64,
}

pub struct Raft {
    storage: Storage, // object to hold this peer's persisted state
    peers: Vec<Client>, // id of all peers
    pub me: i32, // this peer's id, index of peers vec
    leader_id: i32, // leader's id
    pub state: State, // current state of this peer
    apply_ch: SyncSender<ApplyMsg>,

    pub current_term: u64, // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    vote_for: i32, // candidateId that received vote in current term (or -1 if none)
    commit_index: u64, // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    last_applied: u64, // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    log: Vec<LogEntry>, // log entries (first index is 1)

    pub next_index: Vec<usize>, // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    pub match_index: Vec<usize>, // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    last_included_index: u64, // index of the last entry in the log that snapshot replaces (initialized to 0)
    last_included_term: u64, // term of lastIncludedIndex

    election_timer: SyncSender<()>,
    notify_apply_ch: SyncSender<()>,
    message_ch: SyncSender<Message>,

    pub voted_cnt: i32, // voted count during a election
}

impl Raft {
    // create a new raft node.
    pub fn new(
        id: i32,
        prs: Vec<Client>,
        apply_ch: &SyncSender<ApplyMsg>,
    ) -> Arc<Mutex<Raft>> {
        let (ns, nr) = mpsc::sync_channel(1);
        let (ms, mr) = mpsc::sync_channel(1);
        let (ts, tr) = mpsc::sync_channel(1);
        let r = Raft {
            storage: Storage::new(),
            peers: prs,
            me: id,
            leader_id: -1,
            state: Follower,
            apply_ch: apply_ch.clone(),
            current_term: 0,
            vote_for: -1,
            commit_index: 0,
            last_applied: 0,
            log: vec![LogEntry {
                index: 0,
                term: 0,
                command: Vec::new(),
            }],
            next_index: Vec::new(),
            match_index: Vec::new(),
            last_included_index: 0,
            last_included_term: 0,
            notify_apply_ch: ns,
            message_ch: ms,
            voted_cnt: 0,
            election_timer: ts,
        };
        let ret = Arc::new(Mutex::new(r));
        let arc_r = ret.clone();
        // election daemon
        thread::spawn(move || { Self::tick_election(tr, arc_r) });
        ret
    }

    // start to execute a command.
    pub fn start(&self, command: &Vec<u8>) {}

    // implement AppendEntries RPC.
    pub fn append_entries(r: Arc<Mutex<Raft>>, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let reply = AppendEntriesReply {
            success: false,
            term: 0,
            confilct_index: 0,
            confilct_term: 0,
        };
        reply
    }

    // implement RequestVote RPC.
    pub fn request_vote(r: Arc<Mutex<Raft>>, args: &RequestVoteArgs) -> RequestVoteReply {
        let mut reply = RequestVoteReply { term: 0, vote_granted: false };
        let mut rf = r.lock().unwrap();
        reply.vote_granted = false;
        reply.term = rf.current_term;
        if args.term < rf.current_term {
            // reject because candidate expired
            return reply;
        }
        // TODO: log entry inspect

        //if candidate's term is greater, grant
        if args.term > rf.current_term {
            rf.vote_for = -1;
            rf.current_term = args.term;
            reply.term = rf.current_term;
        }

        if rf.vote_for == -1 {
            rf.election_timer.send(());
            rf.state = Follower;
            reply.vote_granted = true;
            rf.vote_for = args.candidate_id;
        }
        reply
    }

    // get current state of Raft.
    pub fn get_state(r: Arc<Mutex<Raft>>) -> (u64, bool) {
        let raft = r.lock().unwrap();
        let term = raft.current_term;
        let is_leader = match raft.state {
            Leader => true,
            _ => false,
        };
        (term, is_leader)
    }

    // kill this peer.
    pub fn kill(r: Arc<Mutex<Raft>>) {}

    // save current Raft state to stable storage.
    fn persist(&self) {}

    // read Raft state from stable storage.
    fn read_persist(&mut self, data: &Vec<u8>) {}

    // leader election.
    fn campaign(r: Arc<Mutex<Raft>>) {
        let mut rf = r.lock().unwrap();
        rf.voted_cnt = 0;
        rf.vote_for = rf.me;
        rf.state = Candidate;
        rf.current_term += 1;
        let last_index = rf.last_index();
        let last_term = rf.log[last_index].term;
//        let args = RequestVoteArgs { term: rf.current_term, candidate_id: rf.me, last_log_index: last_index, last_log_term: last_term };

        // send request to every peer
        for i in 0..rf.peers.len() - 1 {
            if i as i32 == rf.me {
                continue;
            }
            let r1 = r.clone();
            let args = RequestVoteArgs { term: rf.current_term, candidate_id: rf.me, last_log_index: last_index, last_log_term: last_term };
            thread::spawn(move || {
                let r1 = r1;
                let mut rf1 = r1.lock().unwrap();
                match rf1.send_request_vote(i, args) {
                    // got reply
                    Ok(reply) => {
                        if let Candidate = rf1.state {
                            //got voted
                            if reply.vote_granted {
                                rf1.voted_cnt += 1;
                                // win
                                if rf1.voted_cnt as usize == rf1.log.len() / 2 {
                                    rf1.state = Leader;
                                    // initiate leader state
                                    for i in 0..rf1.peers.len() - 1 {
                                        rf1.match_index[i] = 0;
                                        rf1.next_index[i] = rf1.log.len();
                                    }
                                    let me = rf1.me as usize;
                                    rf1.match_index[me] = rf1.last_index();
                                    // tick heart beat
                                    let r1 = r1.clone();
                                    thread::spawn(move || {
                                        Self::tick_heartbeat(r1);
                                    });
                                }
                            } else {
                                if reply.term > rf1.current_term {
                                    rf1.state = Follower;
                                    rf1.election_timer.send(());  // reset timer
                                    rf1.current_term = reply.term;
                                }
                            }
                        }
                    }
                    Err(str) => {
                        println!("no reply while send vote request to {}", i)
                    }
                }
            });
        }
    }

    // call AppendEntries RPC of one peer.
    fn send_append_entries(&self, id: i32) {}

    // call RequestVote RPC of one peer.
    fn send_request_vote(&self, id: usize, args: RequestVoteArgs) -> Result<RequestVoteReply, &'static str> {
//        let reply = RequestVoteReply{term:0, vote_granted:false};
        let req = serialize(&args).unwrap();
        let (reply, success) = self.peers[id].Call(String::from("Raft.RequestVote"), req);
        if success {
            let reply: RequestVoteReply = deserialize(&reply).unwrap();
            return Ok(reply);
        }
        Err("get request vote rpc reply error")
    }

    // send committed log to apply.
    fn apply(r: Arc<Mutex<Raft>>) {}

    // send heartbeat to followers within a given time interval.
    // only call by leader.
    fn tick_heartbeat(r: Arc<Mutex<Raft>>) {
        while true {
            {
                let rf = r.lock().unwrap();
                if let Leader = rf.state {
                    rf.election_timer.send(());  //reset timer
                    // TODO: broadcast
                } else {
                    return;
                }
            } // unlock during sleep
            thread::sleep(Duration::from_millis(HEARBEAT_INTERVAL));
        }
    }

    // start election after timeout.
    fn tick_election(receiver: Receiver<()>, r: Arc<Mutex<Raft>>) {
        while true {
            match receiver.recv_timeout(Self::random_timeout(MIN_TIMEOUT, MAX_TIMEOUT)) {
                Ok(_) => continue,
                Err(timeout) => {
                    println!("timeout, start election!");
                    let r1 = r.clone();
                    thread::spawn(move || { Self::campaign(r1) });
                }
            };
        }
    }

    // send log entries to followers.
    // only call by leader.
    fn replicate(&self) {}

    // send log entries to one follower.
    // only call by leader.
    fn send_log_entries(&self, id: i32) {}

    fn last_index(&self) -> usize {
        self.log.len() - 1
    }

    fn random_timeout(min: u64, max: u64) -> Duration {
        let timeout = rand::thread_rng().gen_range(min, max);
        Duration::from_millis(timeout)
    }
}

pub fn RequestVote(args: Vec<u8>) -> (Vec<u8>, bool) {
    //args *RequestVoteArgs, reply *RequestVoteReply
    let req: RequestVoteArgs = deserialize(&args[..]).unwrap();
    println!("call RequestVote, args:{:?}", args);
    (Vec::new(), true)


    // let reply : RequestVoteReply;
    // let reply = serialize(&reply).unwrap();
    // (reply, true)
}

pub fn AppendEntries(args: Vec<u8>) -> (Vec<u8>, bool) {
    //args *RequestVoteArgs, reply *RequestVoteReply
    let req: AppendEntriesArgs = deserialize(&args[..]).unwrap();
    println!("call AppendEntries, args:{:?}", args);
    (Vec::new(), true)


    // let reply : RequestVoteReply;
    // let reply = serialize(&reply).unwrap();
    // (reply, true)
}

#[cfg(test)]
mod tests {
    use std::thread;

    use super::*;

    #[test]
    fn raft_works() {
        let rpcFunc = rpc::RpcFunc {
            vote: RequestVote,
            append: AppendEntries,
            add_two: |x| {
                x + 2
            },
        };

        let rn1 = rpc::MakeNetwork(String::from("127.0.0.1:7801"), rpcFunc.clone());
        let rn2 = rpc::MakeNetwork(String::from("127.0.0.1:7802"), rpcFunc.clone());
        let rn3 = rpc::MakeNetwork(String::from("127.0.0.1:7803"), rpcFunc.clone());

        // let mut client11 = rpc::make_end(&rn1, String::from("client12"), String::from("127.0.0.1:7801"));
        let mut client12 = rpc::make_end(&rn1, String::from("client12"), String::from("127.0.0.1:7802"));
        let mut client13 = rpc::make_end(&rn1, String::from("client13"), String::from("127.0.0.1:7803"));
        let mut client11 = client12.clone();

        let mut client21 = rpc::make_end(&rn1, String::from("client21"), String::from("127.0.0.1:7801"));
        // let mut client22 = rpc::make_end(&rn1, String::from("client21"), String::from("127.0.0.1:7802"));
        let mut client23 = rpc::make_end(&rn1, String::from("client23"), String::from("127.0.0.1:7803"));
        let mut client22 = client21.clone();

        let mut client31 = rpc::make_end(&rn1, String::from("client31"), String::from("127.0.0.1:7801"));
        let mut client32 = rpc::make_end(&rn1, String::from("client32"), String::from("127.0.0.1:7802"));
        // let mut client33 = rpc::make_end(&rn1, String::from("client33"), String::from("127.0.0.1:7803"));
        let mut client33 = client31.clone();


        let req = RequestVoteArgs {
            term: 123,
            candidate_id: 2,
            last_log_index: 12,
            last_log_term: 4,
        };
        let req = serialize(&req).unwrap();
        client12.Call(String::from("Raft.RequestVote"), req);

        println!("raft test ok");
    }

    #[test]
    fn test_make() {
        let rpcFunc = rpc::RpcFunc {
            vote: RequestVote,
            append: AppendEntries,
            add_two: |x| {
                x + 2
            },
        };
        let rn1 = rpc::MakeNetwork(String::from("127.0.0.1:8000"), rpcFunc.clone());

        let mut client12 = rpc::make_end(&rn1, String::from("client12"), String::from("127.0.0.1:7802"));
        let mut client13 = rpc::make_end(&rn1, String::from("client13"), String::from("127.0.0.1:7803"));
        let (cs, cr) = mpsc::sync_channel(1);
        let clients: Vec<Client> = vec![client12, client13];
        let rf1 = Raft::new(0, clients, &cs);
        thread::sleep(Duration::from_secs(10));
    }
}