mod storage;
mod rpc;
mod util;

use self::storage::Storage;
use self::rpc::Client;
use std::sync::{Arc, Mutex};
use std::thread;
use std::sync::mpsc::{self, RecvTimeoutError, SyncSender, Receiver};

const HEARBEAT_INTERVAL:u64 = 100;
const ELECTION_TIMEOUT:u64 = 1000;

enum State {
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

#[derive(Clone)]
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

pub struct RequestVoteArgs {
    pub term: u64,
    pub candidate_id: u64,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

pub struct RequestVoteReply {
    pub term: u64,
    pub vote_granted: bool,
}

pub struct AppendEntriesArgs {
    pub term: u64,
    pub leader_id: u64,
    pub prev_log_index: u64,
    pub entries: Vec<LogEntry>,
    pub leader_commit: u64,
}

pub struct AppendEntriesReply {
    pub term: u64,
    pub success: bool,
    pub confilct_index: u64,
    pub confilct_term: u64,
}

pub struct Raft {
    storage: Storage, // object to hold this peer's persisted state
    peers: Vec<Client>, // id of all peers
    me: i32, // this peer's id, index of peers vec
    leader_id: i32, // leader's id
    state: State, // current state of this peer
    apply_ch: SyncSender<ApplyMsg>,

    current_term: u64, // latest term server has seen (initialized to 0 on first boot, increases monotonically)
    vote_for: i32, // candidateId that received vote in current term (or -1 if none)
    commit_index: u64, // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    last_applied: u64, // index of highest log entry applied to state machine (initialized to 0, increases monotonically)
    log: Vec<LogEntry>, // log entries (first index is 1)

    next_index: Vec<u64>, // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    match_index: Vec<u64>, // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    last_included_index: u64, // index of the last entry in the log that snapshot replaces (initialized to 0)
    last_included_term: u64, // term of lastIncludedIndex

    notify_apply_ch: SyncSender<()>,
    message_ch: SyncSender<Message>,
}

impl Raft {
    // create a new raft node.
    pub fn new(
        id: i32,
        prs: &Vec<Client>,
        apply_ch: &SyncSender<ApplyMsg>
    ) -> Arc<Mutex<Raft>> {
        let (ns, nr) = mpsc::sync_channel(1);
        let (ms, mr) = mpsc::sync_channel(1);
        let r = Raft {
            storage: Storage::new(),
            peers: prs.clone(),
            me: id,
            leader_id: -1,
            state: State::Follower,
            apply_ch: apply_ch.clone(),
            current_term: 0,
            vote_for: -1,
            commit_index: 0,
            last_applied: 0,
            log: vec![LogEntry {
                index:0,
                term: 0,
                command: Vec::new()
            }],
            next_index: Vec::new(),
            match_index: Vec::new(),
            last_included_index: 0,
            last_included_term: 0,
            notify_apply_ch: ns,
            message_ch: ms,
        };
        Arc::new(Mutex::new(r))
    }

    // start to execute a command.
    pub fn start(&self, command: &Vec<u8>) {
    }

    // implement AppendEntries RPC.
    pub fn append_entries(r: Arc<Mutex<Raft>>, args: &AppendEntriesArgs) -> AppendEntriesReply {
        let reply = AppendEntriesReply{
            success: false,
            term: 0,
            confilct_index: 0,
            confilct_term: 0
        };
        reply
    }

    // implement RequestVote RPC.
    pub fn request_vote(r: Arc<Mutex<Raft>>, args: &RequestVoteArgs) -> RequestVoteReply {
        let reply = RequestVoteReply{term: 0, vote_granted: false};
        reply
    }

    // get current state of Raft.
    pub fn get_state(r: Arc<Mutex<Raft>>) -> (u64, bool) {
        (0, false)
    }

    // kill this peer.
    pub fn kill(r: Arc<Mutex<Raft>>) {
    }

    // save current Raft state to stable storage.
    fn persist(&self) {
    }

    // read Raft state from stable storage.
    fn readPersist(&mut self, data: &Vec<u8>) {
    }

    // leader election.
    fn campaign(r: Arc<Mutex<Raft>>) {
    }

    // call AppendEntries RPC of one peer.
    fn send_append_entries(&self, id: i32) {
    }

    // call RequestVote RPC of one peer.
    fn send_request_vote(&self, id: i32) {
    }

    // send committed log to apply.
    fn apply(r: Arc<Mutex<Raft>>) {
    }

    // send heartbeat to followers within a given time interval.
    // only call by leader.
    fn tick_heartbeat(r: Arc<Mutex<Raft>>) {
    }

    // start election after timeout.
    fn tick_election(r: Arc<Mutex<Raft>>) {
    }

    // send log entries to followers.
    // only call by leader.
    fn replicate(&self) {
    }

    // send log entries to one follower.
    // only call by leader.
    fn sendLogEntries(&self, id: i32) {
    }
}
