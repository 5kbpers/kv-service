use bincode::{serialize, deserialize};

use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::collections::HashMap;
use std::thread;
use std::sync::mpsc::sync_channel;
use std::sync::mpsc::SyncSender;
use std::sync::mpsc::Receiver;
use std::sync::{Mutex, Arc};

use crate::raft::Raft;

const CALLBACK_NUMS : u32 = 2;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct reqMsg {
	end_name : String,
	svc_meth : String,
	args_type : String,
	args : Vec<u8>,
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct replyMsg {
	ok : bool,
	reply : Vec<u8>,
}

#[derive(Clone)]
pub struct Client {
	end_name : String,
	server_addr: String,
	// done : TcpStream,
}

impl Client {
	pub fn new() -> Client {
		Client {
			end_name : String::from(""),
			server_addr : String::from(""),
		}
	}

	pub fn Call(&self, svc_meth : String, args : Vec<u8>) -> (Vec<u8>, bool) {
		let req = reqMsg {
			end_name : self.end_name.clone(),
			svc_meth : svc_meth,
			args_type : String::from("bin"),
			args : args,
		};

	    if let Ok(mut ch) = TcpStream::connect(self.server_addr.clone()) {
			let req = serialize(&req).unwrap();
		    ch.write(&req).unwrap();

		    let mut buffer = [0; 4096];
		    if let Ok(size) = ch.read(&mut buffer) {
				let reply : replyMsg = deserialize(&buffer[..size]).unwrap();    
			    return (reply.reply, reply.ok);
		    }
		    else {
		    	println!("[RPC] read from {} error", &self.server_addr);
		    	return (Vec::new(), false);
		    }
	    }
	    else {
	    	println!("[RPC] can not connect to {}", &self.server_addr);
	    	return (Vec::new(), false);
	    }
	}
}

// #[derive(Debug)]
pub struct Network {
	addr	:	String,
	reliable    :   bool,
	long_delays   :  bool,                        // pause a long time on send on disabled connection
	long_reordering : bool,                        // sometimes delay replies a long time
	ends     :      Mutex<HashMap<String, bool>>,  // ends, by name
	servers    :    HashMap<String, bool>,     // servers, by name
	// enabled   :     HashMap<String, bool>,        // by end name
	// connections  :  HashMap<String, String>, // end_name -> servername
	// endCh    :      (Sender<reqMsg>, Receiver<reqMsg>),
	// done      :     (Sender<()>, Receiver<()>), // closed when Network is cleaned up
	count     :     Mutex<u32>,
	req_send : Vec<SyncSender<Vec<u8>>>,
	reply_recv : Arc<Mutex<Vec<Receiver<(Vec<u8>, bool)>>>>,
}

pub type ANetwork = Arc<Network>;

pub fn make_network(addr : String, req_send : Vec<SyncSender<Vec<u8>>>, reply_recv : Vec<Receiver<(Vec<u8>, bool)>>) -> ANetwork {
	let mut rn = Network {
		addr : addr,
		reliable : true,
		long_delays : false,
		long_reordering : false,
		ends : Mutex::new(HashMap::new()),
		servers : HashMap::new(),
		// enabled : HashMap::new(),
		// connections : HashMap::new(),
		// endCh : channel(),
		// done : channel(),
		count : Mutex::new(0),
		req_send : req_send,
		reply_recv : Arc::new(Mutex::new(reply_recv)),
	};

	rn.servers.insert(String::from("Raft"), true);

	let rn = Arc::new(rn);
	let rnt = rn.clone();
	
	thread::spawn(move || {
		let addr = &rnt.addr;
	    let listener = TcpListener::bind(addr).unwrap();

	    for stream in listener.incoming() {
	        match stream {
	        	Ok(mut streamm) => {
	        		match handle_connection(&rnt, streamm) {
	        			Ok(_) => (),
	        			Err(err) => println!("{:?}", err),
	        		}
	        	},
	        	Err(err) => println!("{:?}", err),
	        }
	        // thread::spawn(move || {
	        // 	loop {
	        // 	}
	        // });
	        // break;
	    }
	});

	rn
}

fn handle_connection(rn : &ANetwork, mut stream: TcpStream) -> Result<(), std::io::Error> {
    let mut buffer = [0; 4096];
    let size = stream.read(&mut buffer)?;
	let req : reqMsg = deserialize(&buffer[..size]).unwrap();    

    let replyMsg = dispatch(rn, req);

    let replyMsg = serialize(&replyMsg).unwrap();
    
    let rcount = stream.write(&replyMsg)?;
    Ok(())
}

fn dispatch(rn : &ANetwork, req : reqMsg) -> replyMsg {
	let mut count = rn.count.lock().unwrap();
	*count += 1;

	let dot = req.svc_meth.find('.').unwrap();

	let serviceName = &req.svc_meth[..dot];
	let methodName = &req.svc_meth[dot+1..];

	if let Some(service) = rn.servers.get(serviceName) {
		match methodName {
			"RequestVote" => {
				rn.req_send[0].send(req.args).unwrap();
				let rch_tmp = rn.reply_recv.clone();
				let rch = rch_tmp.lock().unwrap();

				let (reply, ok) = rch[0].recv().unwrap();
				return replyMsg {
					ok : ok,
					reply : reply,
				};
			},
			"AppendEntries" => {
				rn.req_send[1].send(req.args).unwrap();
				let rch_tmp = rn.reply_recv.clone();
				let rch = rch_tmp.lock().unwrap();

				let (reply, ok) = rch[1].recv().unwrap();
				return replyMsg {
					ok : ok,
					reply : reply,
				};
			},
			_ => {
				println!("labrpc.Server.dispatch(): unknown method {} in {}.{}; expecting one of {:?}",
					serviceName, serviceName, methodName, &rn.servers);
			},
		};
		return replyMsg {
			ok : true,
			reply : Vec::new(),
		};
	} else {
		println!("labrpc.Server.dispatch(): unknown service {} in {}.{}; expecting one of {:?}",
			serviceName, serviceName, methodName, &rn.servers);
		return replyMsg {
			ok : false,
			reply : Vec::new(),
		};
	}
}

pub fn make_end(rn : &ANetwork, end_name : String, server_addr : String) -> Client {
	let mut ends = rn.ends.lock().unwrap();
	if let Some(_) = ends.get(&end_name) {
		panic!("client: {} already exist!", &end_name);
		// return Client;
	} else {
	    // let stream = TcpStream::connect(server_addr.clone()).unwrap();

		let client = Client {
			end_name : end_name.clone(),
			// ch : stream,
			server_addr : server_addr,
			// done : TcpStream,
		};
		ends.insert(end_name, true);
		return client;
	}
}


#[cfg(test)]
mod tests {
	use super::*;
	use std::thread;

	struct RR {
	    a : u32,
	    peers : Vec<Client>,
	    network : ANetwork,
	    reply_sender : Vec<SyncSender<(Vec<u8>, bool)>>,
	    // req_receiver : Vec<Receiver<Vec<u8>>>,
	}

	#[derive(Serialize, Deserialize, PartialEq, Debug)]
	pub struct RequestVoteArgs {
	    pub term: u64,
	    pub candidate_id: u64,
	    pub last_log_index: u64,
	    pub last_log_term: u64,
	}

	#[derive(Serialize, Deserialize, PartialEq, Debug)]
	struct RequestVoteReply {
	    term: u64,
	    vote_granted: bool,
	}

	fn register_callback(r: &Arc<Mutex<RR>>,  mut req_receiver : Vec<Receiver<Vec<u8>>>) {
	    let rr = r.clone();
	    let req_receiver0 = req_receiver.remove(0);
	    thread::spawn(move || {	//RequestVote
	        loop {
	            let args = req_receiver0.recv().unwrap();
	            
	            let reply = RequestVote(&rr, args);
	            let r1 = rr.lock().unwrap();
	            r1.reply_sender[0].send(reply).unwrap();
	        }
	    });
	    let rr = r.clone();
	    let req_receiver1 = req_receiver.remove(0);
	    thread::spawn(move || { //AppendEntries
	        loop {
	            let args = req_receiver1.recv().unwrap();
	            
	            let reply = AppendEntries(&rr, args);
	            let r1 = rr.lock().unwrap();
	            r1.reply_sender[1].send(reply).unwrap();
	        }
	    });
	}

	fn RequestVote(r: &Arc<Mutex<RR>>, args : Vec<u8>) -> (Vec<u8>, bool) {
	    //args *RequestVoteArgs, reply *RequestVoteReply

	    let rr = r.clone();
	    let r1 = rr.lock().unwrap();
	    let req : RequestVoteArgs = deserialize(&args[..]).unwrap();
	    println!("[RPC] call RequestVote, r.a: {}, args: {:?}", r1.a, args);
	    (Vec::new(), true)


	    // let reply : RequestVoteReply;
	    // let reply = serialize(&reply).unwrap();
	    // (reply, true)
	}

	fn AppendEntries(r: &Arc<Mutex<RR>>, args : Vec<u8>) -> (Vec<u8>, bool) {
	    //args *RequestVoteArgs, reply *RequestVoteReply
	    // let req : AppendEntriesArgs = deserialize(&args[..]).unwrap();    
	    println!("call AppendEntries, args:{:?}", args);
	    (Vec::new(), true)


	    // let reply : RequestVoteReply;
	    // let reply = serialize(&reply).unwrap();
	    // (reply, true)
	}

	fn create_server(addr : String) -> Arc<Mutex<RR>> {
	    let mut req_sendv = Vec::new();
	    let mut reply_sendv = Vec::new();
	    let mut req_recvv = Vec::new();
	    let mut reply_recvv = Vec::new();
	    
	    for i in (0..CALLBACK_NUMS) {
		    let (req_send, req_recv) = sync_channel(1);
		    let (reply_send, reply_recv) = sync_channel(1);

		    req_sendv.push(req_send);
		    reply_sendv.push(reply_send);
		    req_recvv.push(req_recv);
		    reply_recvv.push(reply_recv);
	    }

	    let rn1 = make_network(addr, req_sendv, reply_recvv);

	    let r = RR {
	        a : 10,
	        peers : Vec::new(),
	        network : rn1,
	        reply_sender : reply_sendv,
	        // req_receiver : req_recvv,
	    };
	    let ar = Arc::new(Mutex::new(r));
	    register_callback(&ar, req_recvv);

	    ar
	}

	fn create_servers(server_num : u32) -> Vec<Arc<Mutex<RR>>> {
        let mut base_port = 7810;
        let mut addrs = Vec::new();
        for i in (0..server_num) {
            addrs.push(format!("127.0.0.1:{}", base_port));
            base_port += 1;
        }

        let mut servers = Vec::new();
        for addr in &addrs {
            servers.push(create_server(addr.clone()));
        }

        for i in (0..addrs.len()) {
            let r1 = servers[i].clone();
            let mut raft = r1.lock().unwrap();
            let mut clients = Vec::new();
            for j in (0..addrs.len()) {
                if i == j {
                    clients.push(Client::new());
                } else {
                    let client = make_end(&raft.network, format!("client{}to{}", i, j), addrs[j].clone());
                    clients.push(client);
                }
            }
            raft.peers = clients;
        }
        servers
    }

    #[test]
    fn rpc_test() {
	   	let servers = create_servers(3);
        println!("[RPC]:call create servers ok");

        let req =  RequestVoteArgs {
            term : 123,
            candidate_id : 2,
            last_log_index : 12,
            last_log_term : 4,
        };
        let req = serialize(&req).unwrap();

        let araft = servers[0].clone();
        let raft0 = araft.lock().unwrap();
        raft0.peers[1].Call(String::from("Raft.RequestVote"), req);

        println!("[RPC] test ok");
    }
}
