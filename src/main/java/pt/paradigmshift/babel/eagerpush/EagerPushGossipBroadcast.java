package pt.paradigmshift.babel.eagerpush;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import pt.paradigmshift.babel.eagerpush.messages.GossipMessage;
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
 * <p>Optional anti-entropy hooks: when
 * {@code EagerPushGossipBroadcast.SupportAntiEntropy} is set, the protocol
 * fires {@link IdentifiableMessageNotification} on every delivery and serves
 * {@link MissingIdentifiableMessageRequest}s that the anti-entropy protocol
 * generates against peers it detects as out of sync.
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

    /** Property key — TCP bind address. Defaults to the {@code myself} host's address. */
    public static final String PAR_CHANNEL_ADDRESS = "EagerPushGossipBroadcast.Channel.Address";
    /** Property key — TCP bind port. Defaults to the {@code myself} host's port. */
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

    /** Property key — enable local-host port mapping (single-host test deploys). */
    public static final String PAR_LOCAL_SUPPORT = "EagerPushGossipBroadcast.LocalSupport";
    /** Default value for {@link #PAR_LOCAL_SUPPORT}. */
    public static final boolean DEFAULT_LOCAL_SUPPORT = false;

    private final int fanout;
    private final long removeTimeWindow;
    private final int networkPort;
    private final boolean supportAntiEntropy;
    private final boolean localSupport;
    private final Host myself;
    private final int channelId;

    /** Peers we've heard {@code NeighborUp} for but haven't connected to yet. */
    private final Set<Host> pending = new HashSet<>();
    /** Peers we have an active out-connection to. Random-pick targets for forwards. */
    private final Set<Host> connectedNeighbors = new HashSet<>();
    /** Random-access shadow of {@link #connectedNeighbors} for O(1) sampling. */
    private final List<Host> connectedNeighborsList = new ArrayList<>();
    /** Membership-level neighbour set (independent of TCP connection state). */
    private final Set<Host> neighbors = new HashSet<>();

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

    private final Counter sentMessagesCounter;

    /**
     * Construct the protocol and bind a TCP channel. Either both
     * {@code Channel.Address} and {@code Channel.Port} are provided in
     * {@code properties}, or the protocol derives them from the
     * {@code myself} parameter — which must therefore be non-null in the
     * latter case.
     *
     * @param channelName ignored — present for source compatibility with
     *                    upstream call sites; the protocol always uses
     *                    {@link TCPChannel#NAME}
     * @param properties  protocol configuration; see the {@code PAR_*} constants
     * @param myself      this node's {@link Host} identity. Must be non-null
     *                    unless {@code PAR_CHANNEL_ADDRESS} and
     *                    {@code PAR_CHANNEL_PORT} are both present.
     */
    public EagerPushGossipBroadcast(String channelName, Properties properties, Host myself)
            throws IOException, HandlerRegistrationException {
        super(PROTOCOL_NAME, PROTOCOL_ID);

        this.fanout = Integer.parseInt(properties.getProperty(PAR_FANOUT, DEFAULT_FANOUT));
        this.removeTimeWindow = Long.parseLong(
                properties.getProperty(PAR_DELIVERY_TIMEOUT, DEFAULT_DELIVERY_TIMEOUT));
        this.supportAntiEntropy = readBool(properties, PAR_SUPPORT_ANTIENTROPY, DEFAULT_SUPPORT_ANTIENTROPY);
        this.localSupport = readBool(properties, PAR_LOCAL_SUPPORT, DEFAULT_LOCAL_SUPPORT);

        String address = properties.getProperty(PAR_CHANNEL_ADDRESS);
        String port = properties.getProperty(PAR_CHANNEL_PORT);

        if (address == null) {
            Objects.requireNonNull(myself, "myself must be provided when " + PAR_CHANNEL_ADDRESS + " is unset");
            address = myself.getAddress().getHostAddress();
        }
        if (port == null) {
            Objects.requireNonNull(myself, "myself must be provided when " + PAR_CHANNEL_PORT + " is unset");
            this.networkPort = myself.getPort();
            port = Integer.toString(this.networkPort);
        } else {
            this.networkPort = Integer.parseInt(port);
        }
        this.myself = myself != null
                ? myself
                : new Host(java.net.InetAddress.getByName(address), this.networkPort);

        this.sentMessagesCounter = registerMetric(
                new Counter.Builder("SentMessages", Metric.Unit.NONE).build());

        Properties channelProps = new Properties();
        channelProps.setProperty(TCPChannel.ADDRESS_KEY, address);
        channelProps.setProperty(TCPChannel.PORT_KEY, port);
        this.channelId = createChannel(TCPChannel.NAME, channelProps);
        setDefaultChannel(channelId);
        logger.debug("Created channel id={} bound to {}:{}", channelId, address, port);

        registerMessageSerializer(channelId, GossipMessage.MSG_CODE, GossipMessage.serializer);
        registerMessageHandler(channelId, GossipMessage.MSG_CODE, this::uponGossipMessage);

        registerRequestHandler(BroadcastRequest.REQUEST_ID, this::uponBroadcastRequest);
        if (supportAntiEntropy) {
            registerRequestHandler(MissingIdentifiableMessageRequest.REQUEST_ID, this::uponMissingMessageRequest);
        }

        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

        registerChannelEventHandler(channelId, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
        registerChannelEventHandler(channelId, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);
        registerChannelEventHandler(channelId, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
        registerChannelEventHandler(channelId, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
        registerChannelEventHandler(channelId, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
    }

    @Override
    public void init(Properties props) throws HandlerRegistrationException, IOException {
        // Announce that the channel we own can be shared by other protocols.
        triggerNotification(new ChannelAvailableNotification(
                PROTOCOL_ID, PROTOCOL_NAME, channelId, TCPChannel.NAME, myself));
    }

    /* ───────────────────────── Request handlers ──────────────────────── */

    private void uponBroadcastRequest(BroadcastRequest request, short protoID) {
        GossipMessage msg = new GossipMessage(request.getTimestamp(), myself, request.getPayload(), protoID);
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

    private void uponNeighborUp(NeighborUp up, short protoID) {
        logger.debug("NeighborUp: {}", up.getPeer());

        Host h = neighborHost(up.getPeer());
        neighbors.add(h);

        if (!connectedNeighbors.contains(h) && pending.add(h)) {
            openConnection(h);
        }
    }

    private void uponNeighborDown(NeighborDown down, short protoID) {
        Host h = neighborHost(down.getPeer());
        neighbors.remove(h);

        if (removeConnected(h) || pending.remove(h)) {
            closeConnection(h);
        }
    }

    private void uponMissingMessageRequest(MissingIdentifiableMessageRequest req, short protoID) {
        logger.debug("Anti-entropy: {} reports missing {}",
                     req.getDestination(), req.getMessage().getMID());

        Host d = null;
        Host requested = req.getDestination();
        if (connectedNeighbors.contains(requested) || pending.contains(requested)) {
            d = requested;
            logger.debug("Destination matches one of our connections directly");
        } else {
            Host translated = new Host(requested.getAddress(), getEagerPushGossipPort(requested));
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

        pending.remove(h);
        if (neighbors.contains(h)) {
            if (connectedNeighbors.add(h)) {
                connectedNeighborsList.add(h);
            }
        } else {
            // Membership churn raced us; tear the connection back down.
            removeConnected(h);
            closeConnection(h);
        }
    }

    private void uponInConnectionUp(InConnectionUp event, int channelId) {
        logger.debug("Host (in) {} is up", event.getNode());
    }

    private void uponInConnectionDown(InConnectionDown event, int channelId) {
        logger.debug("Connection from {} is down, cause: {}", event.getNode(), event.getCause());
    }

    /* ────────────────────────── Public access ────────────────────────── */

    public Host getHost() {
        return myself;
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
        if (neighbors.contains(h)) {
            removeConnected(h);
            pending.add(h);
            openConnection(h);
        } else {
            removeConnected(h);
            pending.remove(h);
            closeConnection(h);
        }
    }

    private boolean removeConnected(Host h) {
        boolean wasIn = connectedNeighbors.remove(h);
        if (wasIn) {
            connectedNeighborsList.remove(h);
        }
        return wasIn;
    }

    private Host neighborHost(Host peer) {
        return new Host(peer.getAddress(), getEagerPushGossipPort(peer));
    }

    /**
     * Compute the port at which a remote peer's eager-push protocol listens.
     * In normal deployments every node uses {@link #networkPort}; in
     * local single-host test deployments ({@link #localSupport} = true) the
     * port is derived from the membership-protocol port of the peer
     * ({@code peer.port + 1}).
     */
    private int getEagerPushGossipPort(Host host) {
        return localSupport ? host.getPort() + 1 : networkPort;
    }

    private static boolean readBool(Properties p, String key, boolean defaultValue) {
        String v = p.getProperty(key);
        return v == null ? defaultValue : Boolean.parseBoolean(v);
    }
}
