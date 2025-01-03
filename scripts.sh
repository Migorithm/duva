# Set
(printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') | nc localhost 6379

# Set with expiration
(printf '*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$5\r\n30000\r\n') | nc localhost 6379
(printf '*5\r\n$3\r\nSET\r\n$10\r\nsomanyrand\r\n$3\r\nbar\r\n$2\r\npx\r\n$5\r\n30000\r\n') | nc localhost 6379

(printf '*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$10\r\n9999999999\r\n') | nc localhost 6379

(printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') | nc localhost 6379


# Get
(printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n') | nc localhost 6379

# Keys
(printf '*2\r\n$4\r\nKEYS\r\n$3\r\n"*"\r\n') | nc localhost 6379

# SAVE
(printf '*1\r\n$4\r\nSAVE\r\n') | nc localhost 6379

# config
(printf '*3\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n$3\r\nDir\r\n') | nc localhost 6379


# replication connect
cargo run -- --port 6000  & cargo run -- --port 6001 --replicaof localhost:6000               