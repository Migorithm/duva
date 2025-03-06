p ?= 6379
rp ?= 6378
NETCAT = nc 127.0.0.1 $(p)
k = foo
v = bar

define send_command
	( printf $1 )
endef

set:
	$(call send_command, '*3\r\n$$3\r\nSET\r\n$$3\r\n$(k)\r\n$$3\r\n$(v)\r\n')

set_expire:
	$(call send_command, '*5\r\n$$3\r\nSET\r\n$$3\r\n$(k)\r\n$$3\r\n$(v)\r\n$$2\r\npx\r\n$$6\r\n300000\r\n')

get:
	$(call send_command, '*2\r\n$$3\r\nGET\r\n$$3\r\n$(k)\r\n')

keys:
	$(call send_command, '*2\r\n$$4\r\nKEYS\r\n$$1\r\n*\r\n')

save:
	$(call send_command, '*1\r\n$$4\r\nSAVE\r\n')

config:
	$(call send_command, '*3\r\n$$6\r\nCONFIG\r\n$$3\r\nGET\r\n$$3\r\nDir\r\n')

info:
	$(call send_command, '*2\r\n$$4\r\nINFO\r\n$$11\r\nreplication\r\n')

cluster_info:
	$(call send_command, '*2\r\n$$7\r\nCLUSTER\r\n$$4\r\ninfo\r\n')

cluster_forget:
	$(call send_command, '*3\r\n$$7\r\nCLUSTER\r\n$$6\r\nFORGET\r\n$$40\r\n127.0.0.1:6002\r\n')

leader:
	@echo '🔧 Setting up replication with leader on port $(p) and follower on port $(rp)...'
	@mkdir -p local_test/leader
	@echo '🚀 Starting leader node in local_test/leader...'
	@cd local_test/leader && cargo run -- --port $(p)

follower:
	@echo '🚀 Starting follower node in local_test/follower...'
	@mkdir -p local_test/follower
	@cd local_test/follower && cargo run -- --port $(rp) --replicaof 127.0.0.1:$(p)