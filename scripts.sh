# Set
(printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') | nc localhost 6379

# Get
(printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n') | nc localhost 6379