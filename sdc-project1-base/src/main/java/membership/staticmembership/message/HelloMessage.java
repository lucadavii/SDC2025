package membership.staticmembership.message;

import java.io.IOException;
import java.security.PublicKey;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.PublicKeyHelper;

public class HelloMessage extends SignedProtoMessage {

	public final static short MESSAGE_ID = 201;
	
	private final Host myself;
	private final PublicKey pubKey;
	private final long challenge;
	
	public HelloMessage(Host me, PublicKey pubKey, long challenge) {
		super(MESSAGE_ID);
		this.myself = me;
		this.pubKey = pubKey;
		this.challenge = challenge;
	}

	public PublicKey getPubKey() {
		return pubKey;
	}

	public long getChallenge() {
		return challenge;
	}

	public Host getSender() {
		return myself;
	}

	public final static SignedMessageSerializer<HelloMessage> serializer = new SignedMessageSerializer<HelloMessage>() {

		@Override
		public void serializeBody(HelloMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.myself, out);
			
			if(m.pubKey != null) {
				byte[] key = m.pubKey.getEncoded();
				out.writeInt(key.length);
				out.writeBytes(key);
			} else {
				out.writeInt(0);
			}
			
			out.writeLong(m.challenge);
		}

		@Override
		public HelloMessage deserializeBody(ByteBuf in) throws IOException {
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
			
			long challenge = in.readLong();
			
			return new HelloMessage(h, pubKey, challenge);
		}
		
	};
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return HelloMessage.serializer;
	}

}
