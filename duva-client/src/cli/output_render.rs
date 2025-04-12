use duva::domains::query_parsers::query_io::QueryIO;
use duva_client::command::ClientInputKind;

pub fn render_return_per_input(kind: ClientInputKind, query_io: QueryIO) {
    use ClientInputKind::*;
    match kind {
        Ping | Get | IndexGet | Echo | Config | Keys | Save | Info | ClusterForget | Role
        | ReplicaOf | ClusterInfo => match query_io {
            QueryIO::Null => println!("(nil)"),
            QueryIO::SimpleString(value) => println!("{value}"),
            QueryIO::BulkString(value) => println!("{value}"),
            QueryIO::Err(value) => {
                println!("(error) {value}");
            },
            _ => {
                println!("Unexpected response format");
            },
        },
        Del | Exists => {
            let QueryIO::SimpleString(value) = query_io else {
                println!("Unexpected response format");
                return;
            };
            let deleted_count = value.parse::<u64>().unwrap();
            println!("(integer) {}", deleted_count);
        },
        Set => {
            let v = match query_io {
                QueryIO::SimpleString(value) => value,
                QueryIO::Err(value) => {
                    println!("(error) {value}");
                    return;
                },
                _ => {
                    println!("Unexpected response format");
                    return;
                },
            };
            let rindex = v.split_whitespace().last().unwrap();
            // self.latest_known_index = rindex.parse::<u64>().unwrap();
            println!("OK");
        },
        ClusterNodes => {
            let QueryIO::Array(value) = query_io else {
                println!("Unexpected response format");
                return;
            };
            for item in value {
                let QueryIO::BulkString(value) = item else {
                    println!("Unexpected response format");
                    break;
                };
                println!("{value}");
            }
        },
    }
}
