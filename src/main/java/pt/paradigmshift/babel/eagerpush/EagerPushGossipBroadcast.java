package pt.paradigmshift.babel.eagerpush;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.paradigmshift.babel.eagerpush.messages.GossipMessage;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.metrics.Counter;
import pt.unl.fct.di.novasys.babel.metrics.Metric;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.BroadcastDelivery;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.IdentifiableMessageNotification;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.requests.BroadcastRequest;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.requests.MissingIdentifiableMessageRequest;
import pt.unl.fct.di.novasys.babel.protocols.general.notifications.ChannelAvailableNotification;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborDown;
import pt.unl.fct.di.novasys.babel.protocols.membership.notifications.NeighborUp;
import pt.unl.fct.di.novasys.babel.utils.NetworkingUtilities;
import pt.unl.fct.di.novasys.channel.tcp.TCPChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * Reliable eager-push gossip broadcast protocol.
 *
 * <p>On a {@link BroadcastRequest}, the protocol delivers the message locally
 * and forwards it to a configurable random fanout of connected peers. On
 * receipt of a {@link GossipMessage}, peers deliver locally if the message is
 * novel and re-broadcast to their own fanout excluding the immediate sender
 * and the original sender. A sliding delivery-window cache prevents
 * re-delivery of recently-seen messages.
 *
 * <h2>Identity spaces and peer-address resolution</h2>
 *
 * <p>The membership protocol identifies peers by the endpoint of <em>its</em>
 * channel (the <em>membership space</em>); this protocol talks to peers on the
 * endpoint of the <em>gossip</em> channel (the <em>channel space</em>). All
 * internal state ({@code neighbors}, {@code pending},
 * {@code connectedNeighbors}) holds channel-space identities; membership-space
 * identities are translated exactly once, at the two boundaries where they
 * enter — {@link NeighborUp}/{@link NeighborDown} notifications and
 * {@link MissingIdentifiableMessageRequest}s. How that translation works is
 * governed by {@code EagerPushGossipBroadcast.PeerAddressResolution}:
 *
 * <ul>
 * <li><b>{@code offset}</b> (default) — every process binds its gossip channel
 *     at {@code membership port + PortOffset} (offset defaults to {@code 1}),
 *     so a peer's gossip endpoint is derived from its membership endpoint by
 *     adding the same offset. The contract is self-consistent by construction:
 *     this node's own bind port defaults to {@code myself.port + offset}
 *     (where {@code myself} is the <em>membership-space</em> identity), and an
 *     explicitly configured {@code Channel.Port} that violates the declared
 *     offset fails fast at construction.</li>
 * <li><b>{@code fixed}</b> — every process binds its gossip channel at one
 *     uniform, explicitly known port ({@code EagerPushGossipBroadcast.PeerPort},
 *     defaulting to this node's own bind port). Suitable for homogeneous
 *     deployments where membership ports may vary but the gossip port is
 *     identical everywhere.</li>
 * <li><b>{@code shared}</b> — the protocol does not open a channel at all; it
 *     attaches to the channel announced by another protocol (typically the
 *     membership protocol) via {@link ChannelAvailableNotification}. Both
 *     identity spaces coincide and no translation happens. This avoids the
 *     port contract entirely and uses fewer connections, at the cost of
 *     head-of-line blocking on shared TCP connections — prefer it when
 *     broadcast payloads are small. {@link BroadcastRequest}s that arrive
 *     before the channel is announced are queued and flushed on attach.</li>
 * </ul>
 *
 * <p>The resolution mode must be uniform across the deployment: a node in
 * {@code shared} mode listens on its membership channel, which an
 * {@code offset}-mode node would never dial.
 *
 * <p>The legacy boolean {@code EagerPushGossipBroadcast.LocalSupport} is still
 * honoured (with a deprecation warning) when the new key is absent:
 * {@code true} maps to {@code offset} (offset 1) and {@code false} to
 * {@code fixed}, both preserving the historical binding semantics where the
 * channel binds directly to the {@code myself} parameter's port.
 *
 * <h2>Anti-entropy hooks</h2>
 *
 * <p>When {@code EagerPushGossipBroadcast.SupportAntiEntropy} is set, the
 * protocol fires {@link IdentifiableMessageNotification} on every delivery and
 * serves {@link MissingIdentifiableMessageRequest}s that the anti-entropy
 * protocol generates against peers it detects as out of sync.
 *
 * @see GossipMessage
 */
public class EagerPushGossipBroadcast extends GenericProtocol {

    private static final Logger logger = LogManager.getLogger(EagerPushGossipBroadcast.class);

    /**
     * Babel protocol numeric identifier.
     *
     * <p>Bumped from the upstream's 1600 to <b>2400</b> to avoid collision
     * with {@code ConfigurationProtocol} (1600) in {@code stoneflux-edgegateway}.
     */
    public static final short PROTOCOL_ID = 2400;
    /** Babel protocol name. */
    public static final String PROTOCOL_NAME = "EagerPushGossipBroadcast";

    /**
     * Property key — TCP bind address for the protocol's own channel.
     * Falls back, in order, to the Babel-wide defaults
     * {@link Babel#PAR_DEFAULT_ADDRESS} ({@code babel.address}) and
     * {@link Babel#PAR_DEFAULT_INTERFACE} ({@code babel.interface}, resolved
     * to the interface's IPv4 address), and finally to the {@code myself}
     * host's address. Ignored in {@code shared} resolution mode.
     */
    public static final String PAR_CHANNEL_ADDRESS = "EagerPushGossipBroadcast.Channel.Address";
    /**
     * Property key — TCP bind port for the protocol's own channel. When unset:
     * in {@code offset} mode the port is the node's <em>base</em> (membership)
     * port plus {@code PortOffset}, where the base port comes from the
     * {@code myself} host or the Babel-wide default
     * {@link Babel#PAR_DEFAULT_PORT} ({@code babel.port}); in other modes it
     * defaults to {@code myself.port}. Note that {@code babel.port} denotes
     * the membership protocol's port and is therefore never used as a bind
     * port directly — the gossip channel would collide with the membership
     * channel. In {@code offset} mode an explicit value that contradicts the
     * declared offset fails fast. Ignored in {@code shared} resolution mode.
     */
    public static final String PAR_CHANNEL_PORT = "EagerPushGossipBroadcast.Channel.Port";

    /** Property key — fanout (number of peers each broadcast is forwarded to). */
    public static final String PAR_FANOUT = "EagerPushGossipBroadcast.Fanout";
    /** Default fanout: {@value}. */
    public static final String DEFAULT_FANOUT = "4";

    /** Property key — how long a delivered message identifier is remembered. */
    public static final String PAR_DELIVERY_TIMEOUT = "EagerPushGossipBroadcast.DeliveredTimeout";
    /** Default delivery-window timeout: 10 minutes. */
    public static final String DEFAULT_DELIVERY_TIMEOUT = "600000";

    /** Property key — enable anti-entropy hooks. */
    public static final String PAR_SUPPORT_ANTIENTROPY = "EagerPushGossipBroadcast.SupportAntiEntropy";
    /** Default value for {@link #PAR_SUPPORT_ANTIENTROPY}. */
    public static final boolean DEFAULT_SUPPORT_ANTIENTROPY = false;

    /**
     * Property key — peer-address resolution strategy. One of
     * {@link #RESOLUTION_OFFSET}, {@link #RESOLUTION_FIXED} or
     * {@link #RESOLUTION_SHARED}; see the class Javadoc for the semantics of
     * each mode.
     */
    public static final String PAR_PEER_ADDRESS_RESOLUTION = "EagerPushGossipBroadcast.PeerAddressResolution";
    /** Resolution mode — peer gossip port is {@code membership port + PortOffset}. */
    public static final String RESOLUTION_OFFSET = "offset";
    /** Resolution mode — all peers listen at the single uniform {@code PeerPort}. */
    public static final String RESOLUTION_FIXED = "fixed";
    /** Resolution mode — reuse the channel announced by another protocol; no translation. */
    public static final String RESOLUTION_SHARED = "shared";
    /** Default value for {@link #PAR_PEER_ADDRESS_RESOLUTION}: {@value}. */
    public static final String DEFAULT_PEER_ADDRESS_RESOLUTION = RESOLUTION_OFFSET;

    /**
     * Property key — port distance between a process's membership channel and
     * its gossip channel, used (only) in {@code offset} resolution mode both
     * to derive this node's default bind port and to resolve peers.
     */
    public static final String PAR_PORT_OFFSET = "EagerPushGossipBroadcast.PortOffset";
    /** Default value for {@link #PAR_PORT_OFFSET}: {@value}. */
    public static final String DEFAULT_PORT_OFFSET = "1";

    /**
     * Property key — the uniform port every peer's gossip channel listens at,
     * used (only) in {@code fixed} resolution mode. Defaults to this node's
     * own bind port, which preserves the historical behaviour while making
     * the homogeneity assumption explicit and independently overridable.
     */
    public static final String PAR_PEER_PORT = "EagerPushGossipBroadcast.PeerPort";

    /**
     * Property key — in {@code shared} resolution mode, the numeric protocol
     * identifier whose {@link ChannelAvailableNotification} this protocol
     * attaches to. When unset, the first announced channel is adopted; set
     * this whenever more than one protocol in the stack announces a channel.
     */
    public static final String PAR_SHARED_CHANNEL_PROTOCOL = "EagerPushGossipBroadcast.SharedChannelProtocol";

    /**
     * Property key — legacy boolean strategy selector.
     *
     * @deprecated Misleadingly named: it never expressed "local testing" but
     *             rather <em>which port convention to assume for peers</em>.
     *             Use {@link #PAR_PEER_ADDRESS_RESOLUTION} instead
     *             ({@code true} ≙ {@code offset} with offset 1, {@code false}
     *             ≙ {@code fixed}). Honoured, with a warning, only when the
     *             new key is absent.
     */
    @Deprecated
    public static final String PAR_LOCAL_SUPPORT = "EagerPushGossipBroadcast.LocalSupport";

    /** Internal form of the configured resolution strategy. */
    private enum Resolution { OFFSET, FIXED, SHARED }

    private final int fanout;
    private final long removeTimeWindow;
    private final boolean supportAntiEntropy;

    private final Resolution resolution;
    private final int portOffset;
    /** Uniform peer port for {@code fixed} mode; {@code -1} in other modes. */
    private final int peerPort;
    /** Protocol-id filter for shared-channel adoption; {@code -1} = first wins. */
    private final short sharedChannelProtocol;
    /** True when this protocol created (and therefore manages) its channel. */
    private final boolean ownsChannel;

    /**
     * This node's channel-space identity — the endpoint peers reach this
     * protocol at, and the sender stamped into outgoing {@link GossipMessage}s.
     * Fixed at construction when the channel is owned; in {@code shared} mode
     * it is learned from the {@link ChannelAvailableNotification} and may be
     * {@code null} until the channel is announced.
     */
    private Host self;
    /** Channel in use; {@code -1} until a shared channel is adopted. */
    private int channelId = -1;
    /** False only in {@code shared} mode before the channel announcement. */
    private boolean channelReady = false;

    /** Peers we've heard {@code NeighborUp} for but haven't connected to yet. */
    private final Set<Host> pending = new HashSet<>();
    /** Peers we have an active out-connection to. Random-pick targets for forwards. */
    private final Set<Host> connectedNeighbors = new HashSet<>();
    /** Random-access shadow of {@link #connectedNeighbors} for O(1) sampling. */
    private final List<Host> connectedNeighborsList = new ArrayList<>();
    /** Membership-level neighbour set (in channel space, translated on entry). */
    private final Set<Host> neighbors = new HashSet<>();
    /**
     * Every host with a live out-connection on our channel, neighbour or not.
     * On a shared channel the owner's connections fan their events out to us,
     * possibly before the corresponding {@link NeighborUp} arrives; this set
     * lets {@code uponNeighborUp} recognise an already-open connection instead
     * of waiting for an {@code OutConnectionUp} that will never re-fire.
     */
    private final Set<Host> liveOutConnections = new HashSet<>();

    /**
     * Delivery-window cache: FIFO ordering of message identifiers paired with
     * the local wall-clock time at which they were first delivered. Used to
     * deduplicate gossip waves and to age entries out.
     *
     * <p>The upstream's cleanup loop had a use-after-poll bug that removed two
     * entries per iteration (one un-checked); this implementation polls once
     * per iteration.
     */
    private final LinkedList<UUID> receivedInOrder = new LinkedList<>();
    private final Map<UUID, Long> receivedTimestamps = new HashMap<>();

    /** Broadcast requests received in {@code shared} mode before channel adoption. */
    private final List<QueuedBroadcast> queuedBroadcasts = new LinkedList<>();

    private record QueuedBroadcast(BroadcastRequest request, short sourceProto) {
    }

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol and set up its channel according to the
     * configured resolution mode (see the class Javadoc).
     *
     * <p>In {@code offset} and {@code fixed} modes the protocol creates and
     * owns a TCP channel. The bind address comes from
     * {@code Channel.Address}, the Babel-wide {@code babel.address} or
     * {@code babel.interface} defaults, or the {@code myself} parameter; the
     * bind port from {@code Channel.Port} or, in {@code offset} mode, the
     * node's membership base port plus the offset ({@code myself.port},
     * falling back to {@code babel.port}) — see {@link #PAR_CHANNEL_ADDRESS}
     * and {@link #PAR_CHANNEL_PORT}. The {@code myself} parameter may
     * therefore be null whenever the configuration supplies both pieces. In
     * {@code shared} mode no channel is created; the protocol attaches to the
     * first (or filtered, see {@link #PAR_SHARED_CHANNEL_PROTOCOL}) channel
     * announced via {@link ChannelAvailableNotification}.
     *
     * @param channelName ignored — present for source compatibility with
     *                    upstream call sites; an owned channel always uses
     *                    {@link TCPChannel#NAME}
     * @param properties  protocol configuration; see the {@code PAR_*} constants
     * @param myself      this node's {@link Host} identity <em>as known to the
     *                    membership protocol</em> (membership space). Used to
     *                    derive the bind endpoint of an owned channel: in
     *                    {@code offset} mode the channel binds at
     *                    {@code myself.port + offset}, otherwise at
     *                    {@code myself.port}. May be null when the
     *                    configuration supplies the bind endpoint: the address
     *                    via {@code Channel.Address} / {@code babel.address} /
     *                    {@code babel.interface}, and the port via
     *                    {@code Channel.Port} (any mode) or {@code babel.port}
     *                    as the membership base port ({@code offset} mode
     *                    only). Also null-safe in {@code shared} mode, where
     *                    the identity is learned from the channel
     *                    announcement.
     * @throws IllegalArgumentException if {@code Channel.Port} is explicitly
     *                    configured in {@code offset} mode and contradicts
     *                    {@code myself.port + offset} — the offset contract
     *                    peers will assume would be silently violated
     */
    public EagerPushGossipBroadcast(String channelName, Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.fanout = Integer.parseInt(properties.getProperty(PAR_FANOUT, DEFAULT_FANOUT));
        this.removeTimeWindow = Long.parseLong(
                properties.getProperty(PAR_DELIVERY_TIMEOUT, DEFAULT_DELIVERY_TIMEOUT));
        this.supportAntiEntropy = readBool(properties, PAR_SUPPORT_ANTIENTROPY, DEFAULT_SUPPORT_ANTIENTROPY);

        String resolutionProp = properties.getProperty(PAR_PEER_ADDRESS_RESOLUTION);
        String legacyProp = properties.getProperty(PAR_LOCAL_SUPPORT);
        boolean legacyBinding = false;
        if (resolutionProp != null) {
            if (legacyProp != null) {
                logger.warn("Both {} and deprecated {} are set; honouring {}={}",
                        PAR_PEER_ADDRESS_RESOLUTION, PAR_LOCAL_SUPPORT,
                        PAR_PEER_ADDRESS_RESOLUTION, resolutionProp);
            }
            this.resolution = parseResolution(resolutionProp);
        } else if (legacyProp != null) {
            boolean localSupport = Boolean.parseBoolean(legacyProp);
            this.resolution = localSupport ? Resolution.OFFSET : Resolution.FIXED;
            legacyBinding = true;
            logger.warn("{} is deprecated; use {}={} instead",
                    PAR_LOCAL_SUPPORT, PAR_PEER_ADDRESS_RESOLUTION,
                    localSupport ? RESOLUTION_OFFSET : RESOLUTION_FIXED);
        } else {
            this.resolution = parseResolution(DEFAULT_PEER_ADDRESS_RESOLUTION);
        }
        this.portOffset = Integer.parseInt(
                properties.getProperty(PAR_PORT_OFFSET, DEFAULT_PORT_OFFSET));
        this.ownsChannel = this.resolution != Resolution.SHARED;
        String sharedProtoProp = properties.getProperty(PAR_SHARED_CHANNEL_PROTOCOL);
        this.sharedChannelProtocol = sharedProtoProp == null ? -1 : Short.parseShort(sharedProtoProp);

        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        if (supportAntiEntropy) {
            registerRequestHandler(MissingIdentifiableMessageRequest.REQUEST_ID, this::uponMissingMessageRequest);
        }
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

        if (ownsChannel) {
            String address = resolveBindAddress(properties, myself);
            String portProp = properties.getProperty(PAR_CHANNEL_PORT);

            int bindPort;
            if (resolution == Resolution.OFFSET && !legacyBinding) {
                // The node's base port: the membership-space port this node is
                // known by — from myself, or from the Babel-wide default
                // listen port. babel.port is the membership protocol's port,
                // so its only meaningful use here is as the base the offset
                // is added to; it is never a bind port itself.
                Integer basePort = null;
                if (myself != null) {
                    basePort = myself.getPort();
                } else if (properties.getProperty(Babel.PAR_DEFAULT_PORT) != null) {
                    basePort = Integer.parseInt(properties.getProperty(Babel.PAR_DEFAULT_PORT));
                    logger.debug("Using {}={} as this node's membership base port",
                                 Babel.PAR_DEFAULT_PORT, basePort);
                }
                if (portProp != null) {
                    bindPort = Integer.parseInt(portProp);
                    if (basePort != null && bindPort != basePort + portOffset) {
                        throw new IllegalArgumentException(
                                PAR_CHANNEL_PORT + "=" + bindPort + " contradicts the offset contract: "
                                + "peers will assume this node's gossip channel listens at "
                                + (basePort + portOffset) + " (base port " + basePort
                                + " + offset " + portOffset + ")");
                    }
                } else {
                    Objects.requireNonNull(basePort, "either myself, " + Babel.PAR_DEFAULT_PORT
                            + " or " + PAR_CHANNEL_PORT + " must be provided");
                    bindPort = basePort + portOffset;
                }
            } else {
                if (portProp != null) {
                    bindPort = Integer.parseInt(portProp);
                } else {
                    Objects.requireNonNull(myself,
                            "myself must be provided when " + PAR_CHANNEL_PORT + " is unset");
                    bindPort = myself.getPort();
                }
            }
            this.peerPort = resolution == Resolution.FIXED
                    ? Integer.parseInt(properties.getProperty(PAR_PEER_PORT, Integer.toString(bindPort)))
                    : -1;
            this.self = new Host(InetAddress.getByName(address), bindPort);

            Properties channelProps = new Properties();
            channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
            channelProps.setProperty(TCPChannel.PORT_KEY, Integer.toString(bindPort));
            this.channelId = createChannel(TCPChannel.NAME, channelProps);
            setDefaultChannel(channelId);
            registerChannelHandlers();
            this.channelReady = true;
            logger.debug("Created channel id={} bound to {}:{} (resolution={})",
                         channelId, address, bindPort, resolution);
        } else {
            if (properties.getProperty(PAR_CHANNEL_ADDRESS) != null
                    || properties.getProperty(PAR_CHANNEL_PORT) != null) {
                logger.warn("{} / {} are ignored in {}={} mode — the announced channel's binding is used",
                        PAR_CHANNEL_ADDRESS, PAR_CHANNEL_PORT,
                        PAR_PEER_ADDRESS_RESOLUTION, RESOLUTION_SHARED);
            }
            this.peerPort = -1;
            this.self = myself;
            subscribeNotification(ChannelAvailableNotification.NOTIFICATION_ID, this::uponChannelAvailable);
        }

        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        if (ownsChannel) {
            // Announce that the channel we own can be shared by other protocols.
            triggerNotification(new ChannelAvailableNotification(
                    PROTOCOL_ID, PROTOCOL_NAME, channelId, TCPChannel.NAME, self));
        }
    }

    /* ───────────────────────── Request handlers ──────────────────────── */

    private void uponBroadcastRequest(BroadcastRequest request, short protoID) {
        if (!channelReady) {
            queuedBroadcasts.add(new QueuedBroadcast(request, protoID));
            logger.debug("Shared channel not yet announced — queued broadcast request ({} queued)",
                         queuedBroadcasts.size());
            return;
        }
        broadcast(request, protoID);
    }

    private void broadcast(BroadcastRequest request, short protoID) {
        GossipMessage msg = new GossipMessage(request.getTimestamp(), self, request.getPayload(), protoID);
        deliverMessage(msg.clone());

        logger.debug("Received broadcast request: {} targets, fanout {}",
                     connectedNeighborsList.size(), fanout);
        forward(msg, null, null);
        cleanUp();
    }

    private void deliverMessage(GossipMessage msg) {
        receivedInOrder.addLast(msg.getMID());
        receivedTimestamps.put(msg.getMID(), System.currentTimeMillis());

        triggerNotification(new BroadcastDelivery(msg.getSender(), msg.getPayload(), msg.getTimestamp()));
        if (supportAntiEntropy) {
            triggerNotification(new IdentifiableMessageNotification(msg, PROTOCOL_ID));
        }
    }

    /**
     * Garbage-collect entries older than the delivery-window timeout from the
     * dedup cache. The upstream implementation had a use-after-poll bug — the
     * loop polled the head inside the condition AND removed inside the body,
     * dropping two entries per iteration and leaking IDs into the map. This
     * version polls exactly once per iteration.
     */
    private void cleanUp() {
        long staleBarrier = System.currentTimeMillis() - removeTimeWindow;
        while (!receivedInOrder.isEmpty()) {
            UUID head = receivedInOrder.peekFirst();
            Long ts = receivedTimestamps.get(head);
            if (ts == null || ts < staleBarrier) {
                receivedInOrder.pollFirst();
                receivedTimestamps.remove(head);
            } else {
                break;
            }
        }
    }

    /* ───────────────────────── Message handlers ──────────────────────── */

    private void uponGossipMessage(GossipMessage msg, Host sender, short protoID, int cID) {
        if (receivedTimestamps.containsKey(msg.getMID())) {
            logger.trace("Duplicate {} from {} — dropping", msg.getMID(), sender);
            return;
        }
        logger.debug("Got new message {} from {}", msg.getMID(), sender);
        deliverMessage(msg.clone());
        msg.incrementHopCount();
        forward(msg, sender, msg.getSender());
        cleanUp();
    }

    /* ───────────────────── Notification handlers ─────────────────────── */

    /**
     * Adopt a channel announced by another protocol ({@code shared} resolution
     * mode only). The first announcement wins unless
     * {@link #PAR_SHARED_CHANNEL_PROTOCOL} narrows it to a specific owner.
     * Adoption registers our serializer, message handler and channel-event
     * handlers on the shared channel, fixes our channel-space identity from
     * the announced binding, opens connections to any neighbours heard in the
     * meantime and flushes queued broadcast requests.
     */
    private void uponChannelAvailable(ChannelAvailableNotification notification, short protoID) {
        if (channelReady) {
            logger.trace("Ignoring channel announcement from {} — already attached to channel {}",
                         notification.getProtoSourceName(), channelId);
            return;
        }
        if (sharedChannelProtocol >= 0 && notification.getProtoSource() != sharedChannelProtocol) {
            logger.debug("Ignoring channel announcement from {} ({}) — waiting for protocol {}",
                         notification.getProtoSourceName(), notification.getProtoSource(),
                         sharedChannelProtocol);
            return;
        }
        if (!TCPChannel.NAME.equals(notification.getChannelName())) {
            logger.warn("Adopting shared channel of type '{}' — connection events are registered "
                        + "for TCP semantics and may not match", notification.getChannelName());
        }

        this.channelId = notification.getChannelID();
        registerSharedChannel(channelId);
        try {
            registerChannelHandlers();
        } catch (HandlerRegistrationException e) {
            throw new IllegalStateException("Failed to register handlers on shared channel " + channelId, e);
        }

        if (notification.getChannelListenData() != null) {
            this.self = notification.getChannelListenData();
        }
        if (this.self == null) {
            throw new IllegalStateException("Cannot determine local identity: construct with a non-null "
                    + "myself or share a channel that announces its listen endpoint");
        }
        this.channelReady = true;
        logger.info("Attached to shared channel {} ({}) owned by {} ({}); local endpoint {}",
                    channelId, notification.getChannelName(),
                    notification.getProtoSourceName(), notification.getProtoSource(), self);

        for (Host h : pending) {
            openConnection(h);
        }
        for (QueuedBroadcast queued : queuedBroadcasts) {
            broadcast(queued.request(), queued.sourceProto());
        }
        queuedBroadcasts.clear();
    }

    private void uponNeighborUp(NeighborUp up, short protoID) {
        logger.debug("NeighborUp: {}", up.getPeer());

        Host h = resolvePeer(up.getPeer());
        neighbors.add(h);

        if (connectedNeighbors.contains(h)) {
            return;
        }
        if (liveOutConnections.contains(h)) {
            // Shared channel: the owner's connection pre-dates this NeighborUp
            // and its OutConnectionUp already fanned out to us.
            pending.remove(h);
            addConnected(h);
            return;
        }
        if (pending.add(h) && channelReady) {
            openConnection(h);
        }
    }

    private void uponNeighborDown(NeighborDown down, short protoID) {
        Host h = resolvePeer(down.getPeer());
        neighbors.remove(h);

        boolean wasTracked = removeConnected(h) | pending.remove(h);
        // On a shared channel the connection belongs to its owner — likely the
        // very membership protocol still tearing this neighbour down — so only
        // an owned channel closes it.
        if (wasTracked && ownsChannel) {
            closeConnection(h);
        }
    }

    private void uponMissingMessageRequest(MissingIdentifiableMessageRequest req, short protoID) {
        logger.debug("Anti-entropy: {} reports missing {}",
                     req.getDestination(), req.getMessage().getMID());

        if (!channelReady) {
            logger.info("Unable to recover {} to {} — channel not yet available",
                        req.getMessage().getMID(), req.getDestination());
            return;
        }

        // Contract: the destination carries a membership-space identity and is
        // translated exactly once. An exact match against a connection we
        // already track is ground truth and short-circuits the translation
        // (and keeps requests already in channel space working).
        Host d = null;
        Host requested = req.getDestination();
        if (connectedNeighbors.contains(requested) || pending.contains(requested)) {
            d = requested;
            logger.debug("Destination matches one of our connections directly");
        } else {
            Host translated = resolvePeer(requested);
            if (connectedNeighbors.contains(translated) || pending.contains(translated)) {
                d = translated;
                logger.debug("Destination translated to {} matches a connection", translated);
            }
        }

        if (d != null) {
            sendMessage(req.getMessage(), d);
            sentMessagesCounter.inc();
            logger.info("Recovered message {} to {}", req.getMessage().getMID(), d);
        } else {
            logger.info("Unable to recover {} to {}", req.getMessage().getMID(), requested);
        }
    }

    /* ───────────────────────── Channel events ─────────────────────────── */

    private void uponOutConnectionDown(OutConnectionDown event, int channelId) {
        Host h = event.getNode();
        logger.trace("Host {} is down, cause: {}", h, event.getCause());
        reconnectOrDrop(h);
    }

    private void uponOutConnectionFailed(OutConnectionFailed<?> event, int channelId) {
        Host h = event.getNode();
        logger.trace("Connection to {} failed, cause: {}", h, event.getCause());
        reconnectOrDrop(h);
    }

    private void uponOutConnectionUp(OutConnectionUp event, int channelId) {
        Host h = event.getNode();
        logger.trace("Host (out) {} is up", h);

        liveOutConnections.add(h);
        pending.remove(h);
        if (neighbors.contains(h)) {
            addConnected(h);
        } else if (ownsChannel) {
            // Membership churn raced us; tear the connection back down.
            removeConnected(h);
            closeConnection(h);
        } else {
            // Shared channel: a connection the owner opened to a host that is
            // not (or not yet) our neighbour. Mirror it and leave it alone —
            // a later NeighborUp may claim it.
            logger.trace("Connection to non-neighbour {} on shared channel — mirroring only", h);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.debug("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.debug("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* ────────────────────────── Public access ────────────────────────── */

    /**
     * This node's channel-space identity. In {@code shared} resolution mode
     * this may be {@code null} until the shared channel is announced.
     */
    public Host getHost() {
        return self;
    }

    /* ────────────────────────────── Helpers ──────────────────────────── */

    /**
     * Forward {@code msg} to up to {@link #fanout} random peers, excluding
     * {@code immediateSender} (so the message doesn't bounce back) and
     * {@code originalSender} (so we don't loop the message back to its
     * creator on the first hop).
     *
     * <p>Picks without replacement via a partial Fisher-Yates shuffle on a
     * scratch index array — O(fanout) instead of the upstream's O(fanout × n).
     */
    private void forward(GossipMessage msg, Host immediateSender, Host originalSender) {
        int n = connectedNeighborsList.size();
        if (n == 0 || fanout == 0) return;

        int[] idx = new int[n];
        for (int i = 0; i < n; i++) idx[i] = i;

        int picks = Math.min(fanout, n);
        int sent = 0;
        for (int i = 0; i < n && sent < picks; i++) {
            int j = i + ThreadLocalRandom.current().nextInt(n - i);
            int tmp = idx[i]; idx[i] = idx[j]; idx[j] = tmp;
            Host target = connectedNeighborsList.get(idx[i]);
            if (target.equals(immediateSender) || target.equals(originalSender)) {
                continue;
            }
            sendMessage(msg.clone(), target);
            sentMessagesCounter.inc();
            sent++;
        }
        logger.trace("Forwarded {} to {} target(s) of fanout {} (excluded sender/origin)",
                     msg.getMID(), sent, picks);
    }

    private void reconnectOrDrop(Host h) {
        liveOutConnections.remove(h);
        removeConnected(h);
        if (neighbors.contains(h)) {
            pending.add(h);
            if (channelReady) {
                openConnection(h);
            }
        } else {
            pending.remove(h);
            if (ownsChannel) {
                closeConnection(h);
            }
        }
    }

    private void addConnected(Host h) {
        if (connectedNeighbors.add(h)) {
            connectedNeighborsList.add(h);
        }
    }

    private boolean removeConnected(Host h) {
        boolean wasIn = connectedNeighbors.remove(h);
        if (wasIn) {
            connectedNeighborsList.remove(h);
        }
        return wasIn;
    }

    /**
     * Register the {@link GossipMessage} serializer, message handler and
     * TCP connection-event handlers on {@link #channelId}. Called at
     * construction for an owned channel, or on adoption of a shared one —
     * where {@link GossipMessage#MSG_CODE} must be unique among the message
     * codes of every protocol sharing the channel (guaranteed by the
     * workspace's per-protocol ID-pool convention).
     */
    private void registerChannelHandlers() throws HandlerRegistrationException {
        registerMessageSerializer(channelId, GossipMessage.MSG_CODE, GossipMessage.serializer);
        registerMessageHandler(channelId, GossipMessage.MSG_CODE, this::uponGossipMessage);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    /**
     * Translate a membership-space identity into the channel-space identity
     * this protocol dials, according to the configured resolution mode. This
     * is the single point where the two identity spaces meet; see the class
     * Javadoc for the contract of each mode.
     */
    private Host resolvePeer(Host membershipPeer) {
        return switch (resolution) {
            case OFFSET -> new Host(membershipPeer.getAddress(), membershipPeer.getPort() + portOffset);
            case FIXED -> new Host(membershipPeer.getAddress(), peerPort);
            case SHARED -> membershipPeer;
        };
    }

    /**
     * Determine the bind address of an owned channel. Precedence: the
     * protocol-specific {@link #PAR_CHANNEL_ADDRESS}, the Babel-wide
     * {@link Babel#PAR_DEFAULT_ADDRESS}, the IPv4 address of the Babel-wide
     * {@link Babel#PAR_DEFAULT_INTERFACE}, and finally the {@code myself}
     * parameter's address.
     */
    private static String resolveBindAddress(Properties properties, Host myself) throws IOException {
        String address = properties.getProperty(PAR_CHANNEL_ADDRESS);
        if (address == null) {
            address = properties.getProperty(Babel.PAR_DEFAULT_ADDRESS);
        }
        if (address == null) {
            String iface = properties.getProperty(Babel.PAR_DEFAULT_INTERFACE);
            if (iface != null) {
                address = NetworkingUtilities.getAddress(iface);
                if (address == null) {
                    throw new IOException("Could not resolve an IPv4 address for interface '" + iface
                            + "' (" + Babel.PAR_DEFAULT_INTERFACE + ")");
                }
                logger.debug("Resolved {}={} to bind address {}", Babel.PAR_DEFAULT_INTERFACE, iface, address);
            }
        }
        if (address == null) {
            Objects.requireNonNull(myself, "either myself, " + PAR_CHANNEL_ADDRESS + ", "
                    + Babel.PAR_DEFAULT_ADDRESS + " or " + Babel.PAR_DEFAULT_INTERFACE
                    + " must be provided");
            address = myself.getAddress().getHostAddress();
        }
        return address;
    }

    private static Resolution parseResolution(String value) {
        return switch (value.trim().toLowerCase(Locale.ROOT)) {
            case RESOLUTION_OFFSET -> Resolution.OFFSET;
            case RESOLUTION_FIXED -> Resolution.FIXED;
            case RESOLUTION_SHARED -> Resolution.SHARED;
            default -> throw new IllegalArgumentException(
                    PAR_PEER_ADDRESS_RESOLUTION + " must be one of '" + RESOLUTION_OFFSET + "', '"
                    + RESOLUTION_FIXED + "' or '" + RESOLUTION_SHARED + "', got '" + value + "'");
        };
    }

    private static boolean readBool(Properties p, String key, boolean defaultValue) {
        String v = p.getProperty(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v);
    }
}
