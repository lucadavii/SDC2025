package replication.requests;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.ProtoRequest;
import pt.unl.fct.di.novasys.network.data.Host;
import utils.SignaturesHelper;

public class AppendRequest extends ProtoRequest {

	public static final short REQUEST_ID = 901;
	
	private final UUID requestID;
	private final String clientID;
	private Host source;
	private final byte[] clientRequest;
	private byte[] serverSignature;

	
	public AppendRequest(String clientID, Host source, byte[] bs) {
		super(REQUEST_ID);
		this.requestID = UUID.randomUUID();
		this.clientID = clientID;
		this.source = source;
		this.clientRequest = bs;
		this.serverSignature = null;
	}
	
	public AppendRequest(UUID requestID, String clientID, Host source, byte[] cr, byte[] ssign) {
		super(REQUEST_ID);
		this.requestID = requestID;
		this.clientID = clientID;
		this.source = source;
		this.clientRequest = cr;
		this.serverSignature = ssign;
	}
	
	public void sign(PrivateKey key) throws InvalidKeyException, NoSuchAlgorithmException, SignatureException, IOException {
		ByteBuf buf = Unpooled.buffer();
		buf.writeLong(requestID.getMostSignificantBits());
		buf.writeLong(requestID.getLeastSignificantBits());
		buf.writeBytes(clientID.getBytes());
		Host.serializer.serialize(source, buf);
		buf.writeBytes(clientRequest);
		
		buf.resetReaderIndex();
		byte[] payload = new byte[buf.readableBytes()];
		buf.readBytes(payload);
		
		serverSignature = SignaturesHelper.generateSignature(payload, key);
	}
	
	public boolean checkSignature(PublicKey key) throws InvalidKeyException, SignatureException, NoSuchAlgorithmException, IOException {
		ByteBuf buf = Unpooled.buffer();
		buf.writeLong(requestID.getMostSignificantBits());
		buf.writeLong(requestID.getLeastSignificantBits());
		buf.writeBytes(clientID.getBytes());
		Host.serializer.serialize(source, buf);
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

	public Host getSource() {
		return source;
	}

	public byte[] getClientRequest() {
		return clientRequest;
	}
	public Host getSource() {
		return source;
	}
	public byte[] encode() throws IOException {
		ByteBuf buf = Unpooled.buffer();
		buf.writeLong(requestID.getMostSignificantBits());
		buf.writeLong(requestID.getLeastSignificantBits());
		
		byte[] clientIDBytes = clientID.getBytes();
		buf.writeInt(clientIDBytes.length);
		buf.writeBytes(clientIDBytes);
		
		Host.serializer.serialize(source, buf);
		
		buf.writeInt(clientRequest.length);
		buf.writeBytes(clientRequest);
		
		if (serverSignature != null) {
			buf.writeInt(serverSignature.length);
			buf.writeBytes(serverSignature);
		} else {
			buf.writeInt(0);
		}
		
		byte[] arr = new byte[buf.readableBytes()];
		buf.readBytes(arr);
		return arr;
	}

	public static AppendRequest decode(byte[] bytes) throws IOException {
		ByteBuf buf = Unpooled.wrappedBuffer(bytes);
		long mostSigBits = buf.readLong();
		long leastSigBits = buf.readLong();
		UUID requestID = new UUID(mostSigBits, leastSigBits);
		
		int clientIDLength = buf.readInt();
		byte[] clientIDBytes = new byte[clientIDLength];
		buf.readBytes(clientIDBytes);
		String clientID = new String(clientIDBytes);
		
		Host source = Host.serializer.deserialize(buf);
		
		int clientRequestLength = buf.readInt();
		byte[] clientRequest = new byte[clientRequestLength];
		buf.readBytes(clientRequest);
		
		int serverSignatureLength = buf.readInt();
		byte[] serverSignature = null;
		if (serverSignatureLength > 0) {
			serverSignature = new byte[serverSignatureLength];
			buf.readBytes(serverSignature);
		}
		
		return new AppendRequest(requestID, clientID, source, clientRequest, serverSignature);
	}
	
}
