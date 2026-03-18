# PubSub

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
