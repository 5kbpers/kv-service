pub mod client;
pub mod server;
pub mod common;

#[cfg(test)]
mod tests {
    use super::client;
    use super::server;

    #[test]
    fn kv_basic() {
        let addrs = get_addrs(3);
        let mut clients = Vec::new();
        for i in (0..3) {
            clients.push(server::KVServer::new(i as i32, &addrs, 100));
        }
        let mut clerk = client::Clerk::new(&clients, 0);
        println!("put key: key");
        clerk.put(&String::from("key"), &String::from("value"));
        let v = clerk.get(&String::from("key"));
        println!("get value: {}", v);
    }

    fn get_addrs(server_num: usize) -> Vec<String> {
        let mut port = 8000;
        let mut addrs = Vec::new();
        for i in (0..server_num) {
            addrs.push(format!("127.0.0.1:{}", port));
            port += 1;
        }
        return addrs;
    }
}
