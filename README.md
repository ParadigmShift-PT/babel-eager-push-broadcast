# Babel Eager-Push Gossip Broadcast

Reliable eager-push gossip broadcast for [Babel](https://github.com/) applications.
Provided and evolved independently of the original work.

**Group ID:** `pt.paradigmshift.babel`
**Artifact ID:** `eager-gossip-broadcast`
**Current version:** `0.1.0`
**Tested with:** `pt.paradigmshift.babel:babel-core` (Babel-Swarm core fork) and
`pt.paradigmshift.babel:babel-protocols-common` (shared dissemination / membership API surface).
**Source / target:** Java 17.

---

## What it does

Each call to `BroadcastRequest` produces a `GossipMessage` that is

1. delivered locally (a `BroadcastDelivery` notification is fired on the
   originating node), and
2. forwarded to a configurable random *fanout* of connected neighbours.

On receipt, each peer checks whether it has seen the message identifier
recently (within the delivery-window cache); if not, it delivers locally and
re-broadcasts to its own fanout, *excluding* both the immediate sender and
the original `GossipMessage.sender`. This continues until the message has
propagated to every reachable peer.

Eager-push gives low latency and bounded send overhead per node
(`fanout × messages-per-second`) at the cost of some redundancy on edges
that receive the same message from multiple paths. The delivery-window cache
absorbs that redundancy. Pair with `babel-antientropy` when stronger
eventual-delivery guarantees are needed.

## Protocol & event identifiers

This module follows the Babel ID convention used across the ParadigmShift
workspace: protocol IDs at 100-multiples; events numbered per handler class
from `protocol_id + 1` upward, four independent pools.

`EagerPushGossipBroadcast` claims **protocol slot 2400**.

| Type | Handler class | ID | Purpose |
|---|---|---|---|
| `GossipMessage` | message | `2401` | Carries one application-level broadcast (with hop count, original sender, delivery UUID) |

The upstream chose slot 1600, which collides with `ConfigurationProtocol`
in `stoneflux-edgegateway`. This fork moves to 2400 to fit alongside the
gateway's other protocols (1000/1300/1400/1500/1600/1700/1800/1900/2000–2300).

## Configuration

| Property | Default | Description |
|---|---|---|
| `EagerPushGossipBroadcast.Channel.Address` | from `myself` | TCP bind address. Defaults to the address of the `Host` passed to the constructor. |
| `EagerPushGossipBroadcast.Channel.Port`    | from `myself` | TCP bind port. |
| `EagerPushGossipBroadcast.Fanout`          | `4` | Number of random peers each broadcast is forwarded to. |
| `EagerPushGossipBroadcast.DeliveredTimeout`| `600000` ms | How long a delivered message ID is remembered in the dedup cache. |
| `EagerPushGossipBroadcast.SupportAntiEntropy` | `false` | When `true`, fires `IdentifiableMessageNotification` on delivery (consumed by `babel-antientropy`) and handles `MissingIdentifiableMessageRequest` (recovers messages anti-entropy detects as missing on a peer). |
| `EagerPushGossipBroadcast.LocalSupport`    | `false` | When `true`, neighbour port is computed as `peer.port + 1` instead of `this.networkPort` — used in local single-host test deployments where every node is on the same loopback IP. |

## How application protocols plug in

Issue a `BroadcastRequest(timestamp, payload)`; receive `BroadcastDelivery`
notifications on every node when the message arrives. The protocol is
oblivious to the payload shape — it carries opaque bytes.

Membership comes from any protocol that fires `NeighborUp` / `NeighborDown`
— typically HyParView (with discovery) or a static-peer list wrapper.

## Build

```bash
mvn clean install
```

Depends on `babel-sc-core` and `babel-protocol-commons-j21` from the NOVA SYS
Maven repository (`https://novasys.di.fct.unl.pt/packages/mvn`); the
repository is listed in `pom.xml`.

## Tuning notes

- `Fanout` of 4 over a HyParView active view of 5–6 saturates the active view
  on every round, which is the design intent for highest reliability. Lower
  fanout (2–3) shifts the workload toward the lazy paths and is suitable
  when bandwidth is constrained but messages are infrequent.
- `DeliveredTimeout` is a memory ↔ duplicate-tolerance trade-off. Too small
  and the same message can be re-delivered if the gossip wave is slower than
  the timeout; too large and the dedup cache grows. Default 10 minutes is
  comfortable for sub-second-RTT meshes of any size we expect (tens of
  nodes).

## Differences from the upstream

This is a ParadigmShift evolution of the original protocol. Headline changes:

- The "Entrophy" / "Gosssip" / "Tiemeout" typos in property keys, field
  names and log strings are corrected.
- Maven coordinates moved to
  `pt.paradigmshift.babel:eager-gossip-broadcast`. Java package moved
  to `pt.paradigmshift.babel.eagerpush`.
- **Protocol ID bumped from 1600 to 2400** to avoid a collision with the
  `ConfigurationProtocol` slot used by `stoneflux-edgegateway`.
- **Critical fix:** the upstream delivery-window cleanup loop polled the
  head of the queue inside the loop condition *and again* in the loop body,
  so it removed two entries per iteration (one of them unchecked) — leaking
  IDs into `receivedTimestamps` forever. Rewritten as a single-poll loop.
- Random target selection avoids the O(n) `ArrayList.remove(int)` per pick
  by drawing without replacement from an index pool (partial Fisher-Yates).
- Random source switched from `new Random(seed)` per-instance to
  `ThreadLocalRandom.current()`.
- Constructor now refuses a `null` `myself` argument when no
  `Channel.Address` is configured (the upstream would NPE inside the
  channel-properties branch).
- Source layout moved from `src/pt/...` to the conventional Maven
  `src/main/java/pt/...`.
- Public mutable parameter fields demoted to `private final` with getters
  where needed.
- Public javadoc on every public type and method.
