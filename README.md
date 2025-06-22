
<img src="https://github.com/user-attachments/assets/7f82717d-ddfd-4338-a257-072de05c6788" width="300" />




## 📚[Documentation](https://migorithm.github.io/duva/) 
**Duva** is a distributed cache server designed for high performance and scalability. 
It uses the Actor model to manage concurrency, clustering, and consistency, and is built in Rust.

https://github.com/user-attachments/assets/d14a85a7-5edb-4466-8bd0-f538b3d7d27d



## Why the Actor model?
The Actor model is well-suited for concurrent, distributed, and event-driven systems. 
It enables high scalability by modeling independent units of computation (actors) that communicate via message passing.<br>

Key benefits:
- High concurrency: Supports millions of lightweight concurrent entities.
- Event-driven: Naturally fits asynchronous message handling.
- Distributed-friendly: Ideal for systems spanning nodes or data centers.
- Fault-tolerant: Isolates failures and recovers gracefully.




## 💡 Features
The following features have been implemented so far:

- Core Commands inspired by Redis
    - `SET` with optional TTL
    - `GET`
    - `MGET`
    - `KEYS` (supports glob patterns)
    - `SAVE`
    - `EXISTS`
    - `DEL`
    - `INCR`
    - `DECR`
    - `CLUSTER MEET`
    - `CLUSTER FORGET`
    - ...and more
    

- Advanced Features
    - Auto Deletion: Automatically remove expired keys.
    - Local Sharding: Efficiently manage data distribution across local actors.
    - Configurable server behavior
    - Persistence:
        - RDB-like dump (SAVE)
        - Append Only File (AOF) logging
        - <img width="1520" alt="Screenshot 2024-11-23 at 12 02 05 AM" src="https://github.com/user-attachments/assets/0d8b75f6-7a40-4854-9da2-ba98c0ecc3de">
        - Replicated log (in-memory & disk-backed)
    - 🔄 Replica Sync (full + partial)
    - Failure detection via Gossip
    - Follower reads with RYOW consistency 
    - Push-based topology change notification
    - Eviction Policy - LRU(default)
    - Partitioning 
    - Rebalancing - Eager


- Protocol Support
    - RESP Protocol: fully supported for wire compatibility



## 📑 ReplicatedLogs
Duva includes two pluggable replicated log implementations:

### In-Memory Log
- Lightweight and fast
- Ideal for testing or ephemeral environments
- Volatile (data lost on restart)
- Minimal latency and resource overhead

### Disk-Based Segmented Log
- Durable and production-ready
- Implements segmented log pattern
- Active segments for writes, rotated for archival/compaction
- Backed by in-memory index
- Optimizes read performance
- Increases OS page cache hit rate
- Supports high-throughput workloads with persistence
- Both implementations conform to a unified interface, allowing seamless swapping depending on the environment.


## Diagrams
### Client request control
```mermaid
sequenceDiagram
    actor C as Client
    Box Server
        participant Listener
        participant Stream as ClientStream

        participant CL as ClusterActor
        participant CA as CacheActor
    end
    
    loop wait for connection
        activate Listener
        C ->> Listener: Connect
        Listener --) Stream : Create
        deactivate Listener
    end

    loop 
        Stream --)+ Stream: wait & receive request
        rect rgb(108, 161, 166)    

            alt Write Request
                Stream -->> CL: route request
                CL -->> Stream: return response
            end
            Stream -->> CA: route request
            CA -->> Stream: return respons
            Stream -->> C: return response
        end
    end


```


### Clustering
```mermaid

sequenceDiagram
    participant s as Leader    
    actor Cache
    actor Cluster
    actor peer_listener
    
    actor client_listener
    participant Follower
    actor follower_listener

    par 
        s-->>Cache: spawn
        
    and 
        s-->>Cluster: spawn
        Cluster --> Cluster : send heartbeat
        Note right of Cluster : Cluster periodically sends heartbeat to peers 
    and
        s -->>client_listener:spawn
        
    and 
        Follower -->> follower_listener: spawn 
        Note right of Follower : Follower also listens for incoming peer connections
    and 
        s-->>peer_listener: spawn
        loop 
            Follower -->>+ peer_listener: bind 
            peer_listener -->- Follower: threeway handshake
            peer_listener -->> Follower : disseminate peer infomation
            peer_listener -->>+ Cluster : pass stream
            Cluster -->>- Cluster : add peer
        end
        
  
    end

```

### Synchronization on connection
There are quite a few scenarios related to this. For this, take a look at the diagram.
The following is the partial sync scenario on startup:

```mermaid
sequenceDiagram
    participant L as Leader
    participant F as Follower
    participant SF as Second Follower
    L ->> L: Save Empty Dump with (Id, Peer Identifier)
    F ->> L: Connect
    L ->> F: Receive Snapshot
    F ->> F: Save Dump from Leader
    SF ->> L: Connect
    L ->> SF: Receive Snapshot
    SF ->> SF: Save Dump from Leader
    L ->> L: append entry * 5
    L ->> F: Replicate
    L ->> SF: Replicate
    
    L ->> L: Create Snapshot (until hwm 5)
    L ->> F: Create Snapshot 
    L ->> SF : Create Snapshot

    break
        SF --> SF: Second Follower Crashed
    end

    L ->> L: append entry * 3
    L ->> F: Replicate

    SF ->> L: Connect (replid: leader_repl_id, hwm:5, term: 1)
    L ->> SF: Receive Snapshot (hwm: 5)
```


### Partial synchronization for reconcilation
```mermaid
sequenceDiagram
    Actor C as Client
    participant L as Leader
    participant F as Follower
    participant SF as Second Follower
    
    Note over L,SF : Connection established

    C->>L: set x 1 (log 1)

    par 
        L->>F: replicate x 1
    and
        L->>SF: replicate x 1
    end

    C->>L: set y 1 (log 2)

    break
        SF --x SF: crash
    end
    L->>F: replicate y 1

    L -x SF: replicate y 1


    SF -->> SF: recover
    SF -->> L: Join
    L -->> L: store watermark for Second Follower (1)


    L --> L : create append entries individually for each follower
    par 
        L ->> SF : send AppendRPC WITH log 2
    and
        L ->> F : send AppendRPC WITHOUT log
    end

```

### Push-based topology change notification
```mermaid
sequenceDiagram
    actor C as Client
    participant L as Leader
    participant F as Followers

    C --> L : Connection established
    L ->> L : Register client connection to topology change watcher
    
    F --> L : Peer Connection made
    
    L ->> C : Notify client of topology change
    C ->> C : Update topology


    break Leader failed
        L --x L : Crash
        C --x L : request
    end
    
    loop
        C ->> F : Are you leader?
        F ->> C : No
        C ->> F : Are you leader?
        F ->> C : Yes    
    end
    C ->> C : Reset leader information
    C ->> F : retry request


```

### RYOW consistency guarantee (follower reads)
This ensures that clients always see their most recent writes, even when reading from followers—providing a smooth, consistent user experience without unnecessary load on the leader.

This ensures that clients never read stale data after a successful write, improving both performance and consistency without requiring all reads to go to the leader. With this enhancement, Project X delivers stronger guarantees, lower latency, and improved scalability🚀.

```mermaid
sequenceDiagram
    actor C as Client
    participant L as Leader
    participant F as Follower

    note over L,F : X:1<br>hwm:1

    C->>L: write(X,5)
    note over L : X:5<br>hwm:2
    note over F : X:1<br>hwm:1
    L->>C: hwm:2
    C->>F: read(X,hwm=2)
    F--)F: wait for latest hwm
    L->>F: write(X,5,hwm=2)
    note over F : X:5<br>hwm:2
    F->>C: X:5
```

## Strong consistency with Raft

### Election (normal flow)
- Follower becomes candidate after randomized election timeout
- Increments term, votes for self, sends RequestVote
- Majority wins become leader
- Leader sends periodic AppendEntries (heartbeat)

### Split-Brain Handling
- Split votes → no winner
- Retry election after next timeout window


## Failure Detection
The system doesn't cooridnate important decisions that rely on eventual consistency like gossip dissemination. 
However, general information, such as node liveness can be efficiently propagated using such an algorithm.
Duva achieves failure detection using `Gossip mechanism` which may evolve into a hybrid gossip algorithm based on `Plumtree`.
Heartbeat frequency and timeout period before considering a node as failed are highly configurable:
```sh
cargo run -- --hf 100 --ttl 1500
```
Here `hf` means heartbeat is sent every 100ms.
If a peer known to the given node has not sent a heartbeat within 1500ms (ttl), it is considered dead and removed from the list.


## 🚀  Getting Started
### Prerequisites
- Rust (latest stable version)



### Build & Run

```sh
# Standalone
make leader
```

```sh
# cluster

make leader p=6000 tp=repl0 
make follower rp=6001 p=6000 tp=repl1 
make follower rp=6002 p=6000 tp=repl2
```



## Protocol
This server supports the RESP Protocol, enabling interaction with clients in a familiar Redis-like manner.

## 🛣 Roadmap
Future enhancements will include:

- Distributed sharding
- Pub/Sub support
- More advanced data types (e.g., lists, sets, hashes)
- Write-through / read-through support

### Contributing
Contributions are welcome! Please fork the repository and submit a pull request for review.

Currently branch rule is enforcing that your branch matches a pattern that one of below:
- "master"
- "hotfix/*"
- "fix/*"
- "feat/*"
- "chore/*"

### License
This project is licensed under the Apache License 2.0. See the [LICENSE](./LICENSE) file for details.
