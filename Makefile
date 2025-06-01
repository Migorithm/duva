p ?= 6379
rp ?= 6378
NETCAT = nc 127.0.0.1 $(p)
k = foo
v = bar
tp = duva.tp
log_level = debug

leader:
	@echo '🔧 Setting up replication with leader on port $(p) and follower on port $(rp)...'
	@mkdir -p local_test
	@echo '🚀 Starting leader node in local_test...'
	@cd local_test && cargo run --bin duva -- --port $(p) --tpp $(tp) --log_level $(log_level)

leader-aof:
	@echo '🔧 Setting up replication with leader on port $(p) and follower on port $(rp)...'
	@mkdir -p local_test
	@echo '🚀 Starting leader node in local_test...'
	@cd local_test && cargo run --bin duva -- --port $(p) --append_only true --log_level $(log_level)

follower:
	@echo '🚀 Starting follower node in local_test...'
	@mkdir -p local_test
	@cd local_test && cargo run --bin duva -- --port $(rp) --replicaof 127.0.0.1:$(p) --tpp $(tp) --log_level $(log_level)


cli:
	@echo '🚀 Starting client in local_test/cli...'
	cargo run -p duva-client -- --port $(p)