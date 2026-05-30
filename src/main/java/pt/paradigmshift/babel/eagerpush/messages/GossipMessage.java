package pt.paradigmshift.babel.eagerpush.messages;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.messages.IdentifiableProtoMessage;
import pt.unl.fct.di.novasys.babel.protocols.dissemination.notifications.BroadcastDelivery;
import pt.unl.fct.di.novasys.network.ISerializer;
import pt.unl.fct.di.novasys.network.data.Host;

/**
 * One application-level broadcast carried by
 * {@link pt.paradigmshift.babel.eagerpush.EagerPushGossipBroadcast}.
 *
 * <p>The message carries the original sender, the original timestamp, the
 * opaque payload and a hop count that the protocol increments on every relay.
 * Identity is via the inherited {@code UUID} message-id which is what the
 * delivery-window dedup cache keys on.
 *
 * <p>Handler class: message. <b>ID:</b> {@value #MSG_CODE}. Owning protocol:
 * {@code EagerPushGossipBroadcast} (id 2400).
 */
public class GossipMessage extends IdentifiableProtoMessage {

    /** Babel message numeric identifier. */
    public static final short MSG_CODE = 2401;

    private final Timestamp timestamp;
    private final Host sender;
    private final byte[] payload;
    private short hopCount;
    private short protoID;

    /**
     * Construct a fresh broadcast initiated by this node. Assigns a new
     * {@code UUID} message identifier; clones the sender host and payload
     * so subsequent mutations by the caller do not affect the in-flight
     * message.
     */
    public GossipMessage(Timestamp t, Host s, byte[] p, short pID) {
        super(GossipMessage.MSG_CODE);
        this.timestamp = Timestamp.from(t.toInstant());
        this.sender = new Host(s.getAddress(), s.getPort());
        this.payload = p.clone();
        this.hopCount = 0;
        this.protoID = pID;
    }

    /** Deserializer / clone-constructor target. */
    private GossipMessage(UUID mid, long t, Host s, byte[] p, short h, short pID) {
        super(GossipMessage.MSG_CODE, mid);
        this.timestamp = new Timestamp(t);
        this.sender = s;
        this.payload = p;
        this.hopCount = h;
        this.protoID = pID;
    }

    /** Copy-constructor used by {@link #clone()}. */
    private GossipMessage(GossipMessage m) {
        super(GossipMessage.MSG_CODE, m.getMID());
        this.timestamp = m.timestamp;
        this.sender = m.sender;
        this.payload = m.payload;
        this.hopCount = m.hopCount;
        this.protoID = m.protoID;
    }

    @Override
    public BroadcastDelivery generateDeliveryNotification(short sourceProtoID) {
        return new BroadcastDelivery(sender, payload.clone(), new Timestamp(timestamp.getTime()));
    }

    /**
     * Shallow clone that shares the payload byte array. The protocol uses
     * clones so each send-loop iteration carries an independent hop-count
     * mutation state.
     */
    @Override
    public GossipMessage clone() {
        return new GossipMessage(this);
    }

    public short getHopCount() {
        return hopCount;
    }

    public void setHopCount(short hopCount) {
        this.hopCount = hopCount;
    }

    public short getProtoID() {
        return protoID;
    }

    public void setProtoID(short protoID) {
        this.protoID = protoID;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public Host getSender() {
        return sender;
    }

    public byte[] getPayload() {
        return payload;
    }

    /**
     * Increment the hop counter, saturating at {@link Short#MAX_VALUE} so a
     * pathological message ping-ponging forever can't wrap into negative
     * territory.
     *
     * @return the post-increment value
     */
    public short incrementHopCount() {
        if (hopCount < Short.MAX_VALUE) {
            hopCount++;
        }
        return hopCount;
    }

    @Override
    public String toString() {
        return "GossipMessage{"
                + "sender=" + sender
                + ", mid=" + getMID()
                + ", timestamp=" + timestamp
                + ", payload size=" + payload.length
                + ", hopCount=" + hopCount
                + ", protoID=" + protoID
                + '}';
    }

    public static final ISerializer<GossipMessage> serializer = new ISerializer<>() {
        @Override
        public void serialize(GossipMessage msg, ByteBuf out) throws IOException {
            out.writeLong(msg.getMID().getMostSignificantBits());
            out.writeLong(msg.getMID().getLeastSignificantBits());
            Host.serializer.serialize(msg.sender, out);
            out.writeLong(msg.timestamp.getTime());
            out.writeInt(msg.payload.length);
            out.writeBytes(msg.payload);
            out.writeShort(msg.hopCount);
            out.writeShort(msg.protoID);
        }

        @Override
        public GossipMessage deserialize(ByteBuf in) throws IOException {
            UUID mid = new UUID(in.readLong(), in.readLong());
            Host origin = Host.serializer.deserialize(in);
            long t = in.readLong();
            int len = in.readInt();
            if (len < 0 || len > in.readableBytes()) {
                throw new IOException("GossipMessage: payload length out of range: " + len);
            }
            byte[] payload = new byte[len];
            in.readBytes(payload);
            short h = in.readShort();
            short pid = in.readShort();
            return new GossipMessage(mid, t, origin, payload, h, pid);
        }
    };
}
