package broadcast.messages;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;


public class EchoMessage extends  SignedProtoMessage{
    public final static short MESSAGE_ID = 302;

    private Host sender;
    private UUID messageID;
    private final byte[] payload;

    public EchoMessage(Host sender, UUID mID, byte[] payload) {
        super(MESSAGE_ID);
        this.sender = sender;
        this.messageID = mID;
        this.payload = payload;
    }

    public Host getSender() {
        return this.sender;
    }

    public UUID getMessageID() {
        return this.messageID;
    }

    public byte[] getPayload() {
        return this.payload;
    }

    public void setSender(Host sender) {
        this.sender = sender;
    }

    public final static SignedMessageSerializer<EchoMessage> serializer = new SignedMessageSerializer<EchoMessage>() {

        @Override
        public void serializeBody(EchoMessage msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.sender, out);
            out.writeLong(msg.messageID.getMostSignificantBits());
            out.writeLong(msg.messageID.getLeastSignificantBits());
            if(msg.payload != null) {
                out.writeInt(msg.payload.length);
                out.writeBytes(msg.payload);
            } else {
                out.writeInt(0);
            }
        }

        @Override
        public EchoMessage deserializeBody(ByteBuf in) throws IOException {
            Host sender = Host.serializer.deserialize(in);
			UUID id = new UUID(in.readLong(), in.readLong());
			byte[] payload = null;
			int len = in.readInt();
			if( len > 0 ) {
				payload = new byte[len];
				in.readBytes(payload);
			}
			return new EchoMessage(sender, id, payload);
		}
	};

    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return EchoMessage.serializer;
    }
}