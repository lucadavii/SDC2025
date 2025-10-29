package replication.notifications;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import utils.SignaturesHelper;

public class DeliverEntryNotification extends ProtoNotification {

	public static final short NOTIFICATION_ID = 901;
	
	private final UUID requestID;
	private final String clientID;
	private final byte[] clientRequest;
	private byte[] serverSignature;

	
	public DeliverEntryNotification(UUID id, String clientID, byte[] bs, byte[] serverSign) {
		super(NOTIFICATION_ID);
		this.requestID = id;
		this.clientID = clientID;
		this.clientRequest = bs;
		this.serverSignature = serverSign;
	}
	
	public void sign(PrivateKey key) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException {
		ByteBuf buf = Unpooled.buffer();
		buf.writeLong(requestID.getMostSignificantBits());
		buf.writeLong(requestID.getLeastSignificantBits());
		buf.writeBytes(clientID.getBytes());
		buf.writeBytes(clientRequest);
		
		buf.resetReaderIndex();
		byte[] payload = new byte[buf.readableBytes()];
		buf.readBytes(payload);
		
		serverSignature = SignaturesHelper.generateSignature(payload, key);
	}
	
	public boolean checkSignature(PublicKey key) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException {
		ByteBuf buf = Unpooled.buffer();
		buf.writeLong(requestID.getMostSignificantBits());
		buf.writeLong(requestID.getLeastSignificantBits());
		buf.writeBytes(clientID.getBytes());
		buf.writeBytes(clientRequest);
		
		buf.resetReaderIndex();
		byte[] payload = new byte[buf.readableBytes()];
		buf.readBytes(payload);
		
		return SignaturesHelper.checkSignature(payload, serverSignature, key);
	}

	public byte[] getServerSignature() {
		return serverSignature;
	}

	public UUID getRequestID() {
		return requestID;
	}

	public String getClientID() {
		return clientID;
	}

	public byte[] getClientRequest() {
		return clientRequest;
	}

	
	
}
