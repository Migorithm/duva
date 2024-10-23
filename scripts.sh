# Set
(printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') | nc localhost 6379

# Set with expiration
(printf '*5\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$2\r\npx\r\n$2\r\n10\r\n') | nc localhost 6379
(printf '*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n') | nc localhost 6379


# Get
(printf '*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n') | nc localhost 6379