# Babel Eager-Push Gossip Broadcast

Reliable eager-push gossip broadcast for [Babel](https://github.com/) applications.
Provided and evolved independently of the original work.

**Group ID:** `pt.paradigmshift.babel`
**Artifact ID:** `eager-gossip-broadcast`
**Current version:** `0.3.0`
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
absorbs that redundancy. Pair with `broadcast-antientropy` when stronger
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
| `EagerPushGossipBroadcast.Channel.Address` | see description | TCP bind address of the protocol's own channel. Falls back, in order, to the Babel-wide `babel.address`, the IPv4 address of `babel.interface`, and finally the address of the `Host` passed to the constructor. Ignored in `shared` mode. |
| `EagerPushGossipBroadcast.Channel.Port`    | mode-dependent | TCP bind port of the protocol's own channel. When unset: in `offset` mode it is the node's *base* (membership) port + `PortOffset`, the base coming from `myself.port` or the Babel-wide `babel.port`; in other modes it defaults to `myself.port`. `babel.port` denotes the membership protocol's port, so it is never used as a bind port directly (the gossip channel would collide with the membership channel) — its only role is as the base the offset is added to. In `offset` mode an explicit value that contradicts the offset contract fails fast at construction. Ignored in `shared` mode. |
| `EagerPushGossipBroadcast.Fanout`          | `4` | Number of random peers each broadcast is forwarded to. |
| `EagerPushGossipBroadcast.DeliveredTimeout`| `600000` ms | How long a delivered message ID is remembered in the dedup cache. |
| `EagerPushGossipBroadcast.SupportAntiEntropy` | `false` | When `true`, fires `IdentifiableMessageNotification` on delivery (consumed by `broadcast-antientropy`) and handles `MissingIdentifiableMessageRequest` (recovers messages anti-entropy detects as missing on a peer). |
| `EagerPushGossipBroadcast.PeerAddressResolution` | `offset` | How a peer's gossip endpoint is derived from its membership endpoint: `offset`, `fixed` or `shared`. See [Peer address resolution](#peer-address-resolution--channel-modes). |
| `EagerPushGossipBroadcast.PortOffset`      | `1` | `offset` mode only — port distance between each process's membership channel and its gossip channel. |
| `EagerPushGossipBroadcast.PeerPort`        | own bind port | `fixed` mode only — the single uniform port every peer's gossip channel listens at. |
| `EagerPushGossipBroadcast.SharedChannelProtocol` | unset | `shared` mode only — numeric protocol ID whose `ChannelAvailableNotification` to attach to. When unset, the first announced channel is adopted. |
| `EagerPushGossipBroadcast.LocalSupport`    | — | **Deprecated.** Honoured only when `PeerAddressResolution` is absent: `true` ≙ `offset` (offset 1), `false` ≙ `fixed`; both keep the historical binding semantics (channel binds directly to `myself.port`). Logs a deprecation warning. |

## Peer address resolution & channel modes

The membership protocol identifies peers by the endpoint of *its* channel; this
protocol talks to peers on the endpoint of the *gossip* channel. Since the
membership layer is (deliberately) unaware of the gossip protocol's port, the
gossip endpoint of a peer must be derived. All internal state holds
gossip-channel identities; membership identities are translated exactly once,
on entry (`NeighborUp`/`NeighborDown` and anti-entropy recovery requests). The
translation is governed by `EagerPushGossipBroadcast.PeerAddressResolution`,
which **must be configured uniformly across the deployment**:

- **`offset`** (default) — the workspace's port contract, made explicit: every
  process binds its gossip channel at `membership port + PortOffset` (default
  `+1`), so peers are resolved by adding the same offset. The contract is
  self-consistent by construction: the node's own bind port is derived from the
  `myself` (membership) identity plus the offset, and a contradictory explicit
  `Channel.Port` is rejected at construction. Works for distributed and
  single-host deployments alike.
- **`fixed`** — every process binds its gossip channel at one uniform,
  explicitly known port (`PeerPort`). For homogeneous deployments where
  membership ports may vary but the gossip port is identical on every node.
- **`shared`** — no own channel. The protocol attaches to the channel announced
  by another protocol (typically the membership protocol) via
  `ChannelAvailableNotification`; both identity spaces coincide and no
  translation happens. Fewer connections and no port contract, at the cost of
  head-of-line blocking on the shared TCP connections — prefer it when
  broadcast payloads are small. Broadcast requests issued before the channel is
  announced are queued and flushed on attach; on the shared channel the
  protocol only mirrors connection events and never opens/closes connections
  against the owner's lifecycle (it never calls `closeConnection`, and
  `openConnection` only for neighbours not already connected).

When this protocol owns its channel it announces it via
`ChannelAvailableNotification` in `init()`, so other protocols may share *its*
channel in turn.

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
- **`LocalSupport` replaced by `PeerAddressResolution` (0.3.0).** The boolean
  never expressed "local testing" — it selected which port convention to
  assume for peers (`true`: `peer.port + 1`; `false`: every peer at *this*
  node's own port). It is now an explicit three-way strategy (`offset` /
  `fixed` / `shared`, see above) with a declared offset, an explicit uniform
  peer port, fail-fast self-consistency validation, and a new shared-channel
  mode. `LocalSupport` is still honoured (with a warning) when the new key is
  absent. **Note:** the out-of-the-box default changed from the old
  `LocalSupport=false` behaviour to `offset` — configurations that set
  neither key and relied on the implicit "all peers at my port" assumption
  must now set `PeerAddressResolution=fixed`.
