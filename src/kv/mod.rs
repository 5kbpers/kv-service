pub mod client;
pub mod server;
pub mod common;

#[cfg(test)]
mod tests {
    use super::client;
    use super::server;
    use std::thread;
    use std::time::Duration;
    use super::super::raft::rpc::Client;

    #[test]
    fn kv_basic() {
        let addrs = get_addrs(5);
        let mut clients = Vec::new();
        for i in (0..5) {
            let addrs2 = addrs.clone();
            thread::spawn(move||{
                server::KVServer::new(i as i32, &addrs2, 100);
        });
            clients.push(Client{end_name: String::from(""), server_addr: addrs[i].clone()});
        }
        thread::sleep(Duration::from_millis(2000));
        let mut clerk = client::Clerk::new(&clients, 0);
        println!("---------------------put key: key---------------------");
        clerk.put(&String::from("key"), &String::from("value"));
        let v = clerk.get(&String::from("key"));
        println!("get value: {}", v);
        thread::sleep(Duration::from_secs(60));
    }

    fn get_addrs(server_num: usize) -> Vec<String> {
        let mut port = 7000;
        let mut addrs = Vec::new();
        for i in (0..server_num) {
            addrs.push(format!("127.0.0.1:{}", port));
            port += 1;
        }
        return addrs;
    }
}
