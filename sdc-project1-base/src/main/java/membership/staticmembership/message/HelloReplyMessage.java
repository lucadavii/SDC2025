package membership.staticmembership.message;

import java.io.IOException;
import java.security.PublicKey;
import io.netty.buffer.ByteBuf;

import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.PublicKeyHelper;

public class HelloReplyMessage extends SignedProtoMessage {

	public final static short MESSAGE_ID = 202;
	
	private final Host myself;
	private final PublicKey pubKey;
	private final byte[] challengeReply;
	private final long challenge;
	
	public HelloReplyMessage(Host me, PublicKey pubKey, byte[] challengeReply, long challenge) {
		super(MESSAGE_ID);
		this.myself = me;
		this.pubKey = pubKey;
		this.challengeReply = challengeReply;
		this.challenge = challenge;
	}
	
	public Host getSender() {
		return myself;
	}

	public PublicKey getPubKey() {
		return pubKey;
	}

	public byte[] getChallengeReply() {
		return challengeReply;
	}

	public long getChallenge() {
		return challenge;
	}

	public final static SignedMessageSerializer<HelloReplyMessage> serializer = new SignedMessageSerializer<HelloReplyMessage>() {

		@Override
		public void serializeBody(HelloReplyMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.myself, out);
			
			if(m.pubKey != null) {
				byte[] key = m.pubKey.getEncoded();
				out.writeInt(key.length);
				out.writeBytes(key);
			} else {
				out.writeInt(0);
			}
			
			if(m.challengeReply != null) {
				out.writeInt(m.challengeReply.length);
				out.writeBytes(m.challengeReply);
			}
			
			out.writeLong(m.challenge);
		}

		@Override
		public HelloReplyMessage deserializeBody(ByteBuf in) throws IOException {
			Host h = Host.serializer.deserialize(in);
			byte[] key = new byte[in.readInt()];
			
			PublicKey pubKey = null;
			
			if(key.length > 0) {
				try {
					in.readBytes(key);
					pubKey = PublicKeyHelper.rebuildPublicKey(key);
				} catch (Exception e) {
					e.printStackTrace();
					pubKey = null;
				}
			}
			
			byte[] challengeSigned = new byte[in.readInt()];
			
			if(challengeSigned.length > 0) {
				in.readBytes(challengeSigned);
			}
			
			long challenge = in.readLong();
			
			return new HelloReplyMessage(h, pubKey, challengeSigned, challenge);
		}
		
	};
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return HelloReplyMessage.serializer;
	}

}
