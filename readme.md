# Lightcast Pipelines

We have many disparate services and datasets across the company. When we have teams publish data, it would be great for upstream teams that are dependent on that data to be able to kick off automated processes that can ingest or do whatever they need with those datasets. 

This doesn't have to be restrictive to just datasets, this could be any kind of `event` that triggers new jobs or other events.

I think there are multiple components to this system.

1. A Pub Sub service. I need some kind of way to easily publish events to some kind of server. The server then broadcasts to subscribers. Other services can subscribe to these events. The server should have a record of who subscribes to what. Probably needs to store a copy of this on disc (or in a DB) in the event of crashes / outages

2. Some kind of API where I can look at particular services, datasets, and their statuses. Theoretically, there could be multiple events like `dataset-update-start`, `dataset-update-finished`, etc. I want to be able to programatically be able to understand the state of larger scale projects.

## Model

- Data teams publish datasets like they always have. They can opt into the system by emiting "publish" events to the end of their own release pipelines. 

- The Event Bus listens for events, broadcasts to services. Any team can subscribe to events. Ultimately, subscribers are responsible for orchestrating their own pipelines, and doing something useful with the published events. 

- The registry is subscribed to everything. It is a repository of information of meta information. Notifications that are published by the event bus should let the subscribers know where to go to get the information of the newly published dataset (or other things in the future. e.g. models)
```
                                       Data Teams
                                    +-------------+
                                    |   Datasets  |
                                    +-------------+
                                            |
                                      publishes to
                                            v

                                       Event Bus
                                    +-------------+
                                    |     RPC     |
                                    |    Server   |     Notifys Subscribers
                                    |   Session   | --> +-----------------+
                                    |  Transport  |     |   Service A     |
                                    +-------------+ <-- +-----------------+ Subscribes to
                                            |           |   Service B     |
                                         notifys        +-----------------+
                                            v

                                        Registry
                        (subscriptions and service / dataset meta)
                                +---------------------------+
                                |    Registry + State API   |
                                | Subscriptions + metadata  |
                                |  dataset / service status |
                                +---------------------------+
```

##  Refined Model

### Producers

- Emit events at end of pipeline: dataset.version.published
- Include pointers: where the artifact lives (S3 path, catalog id, version hash), schema version, etc.

### Event Bus

- Accept events, validate schema, assign event_id
- Persist events durably (append-only)
- Deliver events to subscribers:
    - push: webhooks
    - pull: long-poll / streaming / “get next events since offset”
- Track subscriber progress (offsets/acks)
- Retries + DLQ

### Registry responsibilities

- Store:
    - subscriptions (filters, endpoints, owners)
    - dataset/service metadata
    - derived state (latest version, last success, last failure, etc.)

- Expose query APIs:
    - “latest published version of dataset X”
    - “who subscribes to dataset X events”
    - “runs/events for correlation_id/run_id”

### Subscribers

- Receive events → orchestrate their own jobs
- Must be idempotent (duplicate events happen)

```
Data Teams
   |
   | publish event (rpc)
   v
+---------------------------+
|        Event Bus          |
| +-----------------------+ |
| | Ingress + Validation  | |
| +-----------------------+ |
| | Durable Event Store   | |
| +-----------------------+ |
| | Delivery + Retries    |-+--> Service A
| | Offsets + DLQ         |-+--> Service B
+---------------------------+
            |
            | Project / Derive State
            v
+---------------------------+
|    Registry + State API   |
| Subscriptions + metadata  |
|  dataset / service status |
+---------------------------+
```

## Event Bus Model

The event bus is a three layer model that builds upon itself. Below are the explicit responsibilities of each layer

### Transport layer

Moves bytes between peers

The transport owns:
- TCP connections
- Framing (length prefix, delimiters)
- Optional TLS
- timeouts / keepalives

The transport layer should not worry about:
- authentication/authorization
- retries
- subscriber logic
- topics / event types

### Session layer

Represents an authenticated stateful connection and provide control above transport

The session owns:
- handshake (client identity, auth token, capability negotiation)
- streams/requests over one connection
- per-session rate limits
- session-scoped subscriptions

The session does not worry about:
- durability / replay (event store / bus core)
- business schemas (rpc layer)

### RPC Server layer

Exposes a clean API that services use

Contains four groups of functions:

*Admin*
- Status()
- Health()
- Meta()
- Metrics()

*Publish*
- PublishEvent(event)

*Subscribe*
- Subscribe(filter)
- Ack()

*Registry*
- ListSubscriptions()

```

+-----------------+
|       RPC       | (Publish, Subscribe, Ack, List)
+-----------------+
         |
         v
+-----------------+
|    Session      | (auth, multiplexing, session subscriptions)
|  connection IDs |
+-----------------+
         |
         v
+-----------------+
|    Transport    | (tcp/quic, tls, framing)
| Sockets + Bytes |
+-----------------+


(not an official layer)
+-----------------+
|    event log    | (durability, ordering)
+-----------------+
```

## Message Structure

```
+------------------------ Transport Frame --------------------+
|                                                             |
|             Transport Header (framing only)                 |
|             - Magic / Version                               |
|             - HeaderLen (bytes)                             |
|             - BodyLen   (bytes)                             |
|             - CRC / checksum (optional)                     |
|                                                             |
| +---------------------- Session Envelope -----------------+ |
| |                                                         | |
| |           Session Header (routing + multiplexing)       | |
| |           - SessionID     (u64)                         | |
| |           - StreamID      (u32)                         | |
| |           - Kind          (u8)                          | |
| |                                                         | |
| |           - Flags         (u8)                          | |
| |           - Seq           (u64)                         | |
| |           - ReqID         (u64)                         | |
| |                                                         | |
| | +-------------------- Body (by Kind) -------------------+ |
| | |                                                       | |
| | |  If Kind == EVENT (server -> subscriber push)         | |
| | |  +-------------------------------------------------+  | |
| | |  |              Event Payload (encoded)            |  | |
| | |  |  - EventID / Offset (u64)                       |  | |
| | |  |  - EventType (string or interned id)            |  | |
| | |  |  - EntityType / EntityID                        |  | |
| | |  |  - ProducedAt                                   |  | |
| | |  |  - ManifestURI / Artifact pointer (optional)    |  | |
| | |  |  - Payload bytes (schema'd per EventType)       |  | |
| | |  +-------------------------------------------------+  | |
| | |                                                       | |
| | |  If Kind == ACK (subscriber -> server)                | |
| | |  +-------------------------------------------------+  | |
| | |  |                Ack Payload (encoded)            |  | |
| | |  |  - StreamID (u32)                               |  | |
| | |  |  - AckOffset/EventID (u64)                      |  | |
| | |  +-------------------------------------------------+  | |
| | |                                                       | |
| | |  If Kind == RPC_REQ / RPC_RES (either direction)      | |
| | |  +-------------------------------------------------+  | |
| | |  |              RPC Payload (encoded)              |  | |
| | |  |  - Method / Status                              |  | |
| | |  |  - RPC-specific bytes                           |  | |
| | |  +-------------------------------------------------+  | |
| | |                                                       | |
| | |  If Kind == HEARTBEAT (either direction)              | |
| | |  +-------------------- empty / tiny ----------------+ | |
| | +-------------------------------------------------------+ |
| +-----------------------------------------------------------+
+--------------------------------------------------------------
```
