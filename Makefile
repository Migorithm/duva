p ?= 6379
rp ?= 6378
NETCAT = nc 127.0.0.1 $(p)
k = foo
v = bar
tp = duva.tp

cli:
	@echo '🚀 Starting client in local_test/cli...'
	@mkdir -p local_test/cli
	@cd local_test/cli && cargo run --bin cli -- --port $(p)

leader:
	@echo '🔧 Setting up replication with leader on port $(p) and follower on port $(rp)...'
	@mkdir -p local_test/leader
	@echo '🚀 Starting leader node in local_test/leader...'
	@cd local_test/leader && cargo run --bin duva -- --port $(p)

leader-tp:
	@mkdir -p local_test/leader
	@cd local_test/leader && cargo run --bin duva -- --port $(p) --topology_path $(tp)

leader-aof:
	@echo '🔧 Setting up replication with leader on port $(p) and follower on port $(rp)...'
	@mkdir -p local_test/leader
	@echo '🚀 Starting leader node in local_test/leader...'
	@cd local_test/leader && cargo run --bin duva -- --port $(p) --append_only true

follower:
	@echo '🚀 Starting follower node in local_test/follower...'
	@mkdir -p local_test/follower
	@cd local_test/follower && cargo run --bin duva -- --port $(rp) --replicaof 127.0.0.1:$(p)

follower-tp:
	@mkdir -p local_test/follower
	@cd local_test/follower && cargo run --bin duva -- --port $(rp) --replicaof 127.0.0.1:$(p) --topology_path $(tp)