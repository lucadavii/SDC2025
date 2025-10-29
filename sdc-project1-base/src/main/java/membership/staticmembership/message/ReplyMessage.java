package membership.staticmembership.message;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class ReplyMessage extends SignedProtoMessage {

	public final static short MESSAGE_ID = 203;
	
	private final Host myself;
	private final byte[] challengeReply;

	
	public ReplyMessage(Host me, byte[] challengeReply) {
		super(MESSAGE_ID);
		this.myself = me;
		this.challengeReply = challengeReply;
	}

	public byte[] getChallengeReply() {
		return challengeReply;
	}

	public Host getSender() {
		return myself;
	}

	public static final SignedMessageSerializer<ReplyMessage> serializer = new SignedMessageSerializer<ReplyMessage>() {

		@Override
		public void serializeBody(ReplyMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.myself, out);
			
			if(m.challengeReply != null) {
				out.writeInt(m.challengeReply.length);
				out.writeBytes(m.challengeReply);
			}
			
		}

		@Override
		public ReplyMessage deserializeBody(ByteBuf in) throws IOException {
			Host h = Host.serializer.deserialize(in);
			
			byte[] challengeSigned = new byte[in.readInt()];
			
			if(challengeSigned.length > 0) {
				in.readBytes(challengeSigned);
			}
			
			return new ReplyMessage(h, challengeSigned);
		}
		
	};
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return ReplyMessage.serializer;
	}

}
