package broadcast.notifications;

import java.io.IOException;
import java.security.PublicKey;
import java.security.Signature;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class DeliveryNotification extends ProtoNotification {

	public final static short NOTIFICATION_ID = 301;
	
	private final Host sender;
	private final byte[] payload;
	private final byte[] signature;
	
	public DeliveryNotification(Host h, byte[] payload, byte[] signature) {
		super(NOTIFICATION_ID);
		this.sender = h;
		this.payload = payload;
		this.signature = signature;
	}
	
	public Host getOriginalSender() {
		return this.sender;
	}
	
	public byte[] getPayload() {
		return this.payload;
	}
	
	public byte[] getSignature() {
		return this.signature;
	}
	
	public boolean checkSignature(PublicKey key, String algorithm) throws Exception {
		Signature sig = Signature.getInstance(algorithm);
		sig.initVerify(key);
		sig.update(sender.toString().getBytes());
		sig.update(payload);
		return sig.verify(signature);
	}
	
	public static DeliveryNotification fromMessage(byte[] message) throws IOException {
		ByteBuf out = Unpooled.wrappedBuffer(message);
		Host h = Host.serializer.deserialize(out);
		int len = out.readInt();
		byte[] payload = new byte[len];
		out.readBytes(payload);
		len = out.readInt();
		byte[] sig = new byte[len];
		out.readBytes(sig);
		return new DeliveryNotification(h, payload, sig);
	}
	
}
