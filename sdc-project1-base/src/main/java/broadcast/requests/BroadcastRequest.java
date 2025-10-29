package broadcast.requests;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.Signature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class BroadcastRequest extends ProtoRequest {

	public final static short REQUEST_ID = 301;
	
	private final Host sender;
	private final byte[] payload;
	private byte[] signature;
	
	public BroadcastRequest(Host sender, byte[] payload) {
		super(REQUEST_ID);
		this.sender = sender;
		this.payload = payload;
		this.signature = null;
	}
	
	public BroadcastRequest(Host sender, byte[] payload, PrivateKey key, String algorithm) {
		this(sender, payload);
		try {
			generateSignature(key, algorithm);
		} catch (Exception e) {
			e.printStackTrace(); 
		}
	}
	
	public BroadcastRequest(Host sender, byte[] payload, PrivateKey key) {
		this(sender, payload);
		try {
			generateSignature(key, SignaturesHelper.SignatureAlgorithm);
		} catch (Exception e) {
			e.printStackTrace(); 
		}
	}
	
	public void generateSignature(PrivateKey key, String algorithm) throws Exception {
		Signature sig = Signature.getInstance(algorithm);
		sig.initSign(key);
		sig.update(this.sender.toString().getBytes());
		sig.update(this.payload);
		this.signature = sig.sign();
	}
	
	public byte[] encode() throws IOException {
		ByteBuf buffer = ByteBufAllocator.DEFAULT.buffer();
		try {
			Host.serializer.serialize(sender, buffer);
			buffer.writeInt(payload.length);
			buffer.writeBytes(payload);
			buffer.writeInt(signature.length);
			buffer.writeBytes(signature);
			byte[] result = new byte[buffer.readableBytes()];
			buffer.resetReaderIndex();
			buffer.readBytes(result);
			return result;
		} finally {
			if (buffer != null) {
				buffer.release();
			}
		}
	}
}
