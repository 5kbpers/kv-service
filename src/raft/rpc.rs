use bincode::{serialize, deserialize};

use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::collections::HashMap;
use std::thread;
use std::sync::{Mutex, Arc};

use crate::raft::Raft;

#[derive(Debug, Clone)]
pub struct RpcFunc {
	// pub vote : fn(Raft, Vec<u8>) -> (Vec<u8>, bool),
	// pub append : fn(Raft, Vec<u8>) -> (Vec<u8>, bool),
	pub vote : fn(Vec<u8>) -> (Vec<u8>, bool),
	pub append : fn(Vec<u8>) -> (Vec<u8>, bool),
	pub add_two : fn(u32) -> (u32),
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct reqMsg {
	endname : String,
	svcMeth : String,
	argsType : String,
	args : Vec<u8>,
}


#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct replyMsg {
	ok : bool,
	reply : Vec<u8>,
}

#[derive(Clone)]
pub struct Client {
	endname : String,
	server_addr: String,
	// done : TcpStream,
}

impl Client {
	pub fn Call(&mut self, svcMeth : String, args : Vec<u8>) -> (Vec<u8>, bool) {
		let req = reqMsg {
			endname : self.endname.clone(),
			svcMeth : svcMeth,
			argsType : String::from("bin"),
			args : args,
		};

	    let mut ch = TcpStream::connect(self.server_addr.clone()).unwrap();

		let req = serialize(&req).unwrap();
	    ch.write(&req).unwrap();

	    let mut buffer = [0; 4096];
	    let size = ch.read(&mut buffer).unwrap();
		let reply : replyMsg = deserialize(&buffer[..size]).unwrap();    
	    // println!("recv reply: {:?}", &reply);
	    let buffer = (&buffer[..size]).to_vec();
	    (buffer, reply.ok)
	}
}

#[derive(Debug)]
pub struct Network {
	addr	:	String,
	reliable    :   bool,
	longDelays   :  bool,                        // pause a long time on send on disabled connection
	longReordering : bool,                        // sometimes delay replies a long time
	ends     :      Mutex<HashMap<String, bool>>,  // ends, by name
	servers    :    HashMap<String, bool>,     // servers, by name
	// enabled   :     HashMap<String, bool>,        // by end name
	// connections  :  HashMap<String, String>, // endname -> servername
	// endCh    :      (Sender<reqMsg>, Receiver<reqMsg>),
	// done      :     (Sender<()>, Receiver<()>), // closed when Network is cleaned up
	count     :     Mutex<u32>,
	rpcFunc : RpcFunc,
}

type ANetwork = Arc<Network>;

pub fn MakeNetwork(addr : String, rpcFunc : RpcFunc) -> ANetwork {
	let mut rn = Network {
		addr : addr,
		reliable : true,
		longDelays : false,
		longReordering : false,
		ends : Mutex::new(HashMap::new()),
		servers : HashMap::new(),
		// enabled : HashMap::new(),
		// connections : HashMap::new(),
		// endCh : channel(),
		// done : channel(),
		count : Mutex::new(0),
		rpcFunc : rpcFunc,
	};

	rn.servers.insert(String::from("Raft"), true);

	let rn = Arc::new(rn);
	let rnt = rn.clone();
	thread::spawn(move || {
		let addr = &rnt.addr;
	    let listener = TcpListener::bind(addr).unwrap();

		// loop {
		// 	let (stream, addr_) = listener.accept().unwrap();
		// 	 thread::spawn(move || {
	 //        	loop {
		// 	        handle_connection(&rnt2, stream);
	 //        	}
	 //        });
		// }

	    for stream in listener.incoming() {
	        let stream = stream.unwrap();
			        handle_connection(&rnt, stream);
	        // thread::spawn(move || {
	        // 	loop {
	        // 	}
	        // });
	        // break;
	    }
	});

	rn
}

fn handle_connection(rn : &ANetwork, mut stream: TcpStream) {
    let mut buffer = [0; 4096];
    let size = stream.read(&mut buffer).unwrap();
	let req : reqMsg = deserialize(&buffer[..size]).unwrap();    

    let replyMsg = dispatch(rn, req);

    let replyMsg = serialize(&replyMsg).unwrap();
    
    let rcount = stream.write(&replyMsg).unwrap();
}

fn dispatch(rn : &ANetwork, req : reqMsg) -> replyMsg {
	let mut count = rn.count.lock().unwrap();
	*count += 1;

	let dot = req.svcMeth.find('.').unwrap();

	let serviceName = &req.svcMeth[..dot];
	let methodName = &req.svcMeth[dot+1..];

	if let Some(service) = rn.servers.get(serviceName) {
		println!("dispatch {} : {}", serviceName, service);

		match methodName {
			"RequestVote" => {
				// let res = (rn.rpcFunc.add_two)(12);
				// println!("call res:{}", res);
				let (reply, ok) = (rn.rpcFunc.vote)(req.args);
				return replyMsg {
					ok : ok,
					reply : reply,
				};
			},
			"AppendEntries" => {
				let (reply, ok) = (rn.rpcFunc.append)(req.args);
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

pub fn make_end(rn : &ANetwork, endname : String, server_addr : String) -> Client {
	let mut ends = rn.ends.lock().unwrap();
	if let Some(_) = ends.get(&endname) {
		panic!("client: {} already exist!", &endname);
		// return Client;
	} else {
	    // let stream = TcpStream::connect(server_addr.clone()).unwrap();

		let client = Client {
			endname : endname.clone(),
			// ch : stream,
			server_addr : server_addr,
			// done : TcpStream,
		};
		ends.insert(endname, true);
		return client;
	}
}



#[cfg(test)]
mod tests {
	use super::*;
	use std::thread;

    #[test]
    fn rpc_test() {
    	let rpcFunc = RpcFunc {
    		vote : |x| {
    			(Vec::new(), true)
    		},
    		append : |x| {
    			(Vec::new(), true)
    		},
    		add_two : |x| {
    			x + 2
    		},
    	};
	    let rn = MakeNetwork(String::from("127.0.0.1:7878"), rpcFunc);
	    
	    let rn1 = rn.clone();
        let h2 = thread::spawn(move || {
        	let req = reqMsg{
        		endname : String::from("endname"),
				svcMeth : String::from("server1.method1"),
				argsType : String::from("argsType"),
				args : vec![1, 2, 3],
        	};
        	let req = serialize(&req).unwrap();

        	let mut client = make_end(&rn1, String::from("client1"), String::from("127.0.0.1:7878"));
        	client.Call(String::from("server123.meth1"), vec![23,2,32]);
	        // make_client("127.0.0.1:7878", &req[..]);
	    });

	    let rn1 = rn.clone();
	 	let h3 = thread::spawn(move || {
	        let req = reqMsg{
        		endname : String::from("endname"),
				svcMeth : String::from("Raft.method2"),
				argsType : String::from("argsType"),
				args : vec![1, 2, 3],
        	};
        	let req = serialize(&req).unwrap();

        	let mut client = make_end(&rn1, String::from("client2"), String::from("127.0.0.1:7878"));
        	client.Call(String::from("Raft.RequestVote"), vec![223,22,32]);
        	client.Call(String::from("Raft.meth23452"), vec![2,22,32]);
	    });

	    h2.join().unwrap();
	    h3.join().unwrap();
	       
    }
}
