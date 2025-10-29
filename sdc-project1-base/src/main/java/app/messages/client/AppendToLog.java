package app.messages.client;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class AppendToLog extends SignedProtoMessage {

	public final static short MESSAGE_ID = 601;
	
	private final String clientID;
	private final UUID requestID;
	
	private final String logEntry;
	private final long timestamp;
	
	public AppendToLog(String clientID, String logEntry) {
		super(MESSAGE_ID);
		this.clientID = clientID;
		this.requestID = UUID.randomUUID();
		this.logEntry = logEntry;
		this.timestamp = System.currentTimeMillis();
		
	}
	
	public AppendToLog(String clientID, UUID requestID, String logEntry, long timestamp) {
		super(MESSAGE_ID);
		this.clientID = clientID;
		this.requestID = requestID;
		this.logEntry = logEntry;
		this.timestamp = timestamp;
	}

	public static SignedMessageSerializer<AppendToLog> serializer = new SignedMessageSerializer<AppendToLog>() {
		
		@Override
		public void serializeBody(AppendToLog msg, ByteBuf out) throws IOException {
			out.writeInt(msg.clientID.length());
			out.writeBytes(msg.clientID.getBytes());
			
			out.writeLong(msg.requestID.getMostSignificantBits());
			out.writeLong(msg.requestID.getLeastSignificantBits());
			
			out.writeLong(msg.timestamp);
			out.writeInt(msg.logEntry.length());
			out.writeBytes(msg.logEntry.getBytes());
		}
		
		@Override
		public AppendToLog deserializeBody(ByteBuf in) throws IOException {
			int len = in.readInt();
			
			byte[] buffer = new byte[len];
			in.readBytes(buffer);
			String cID = new String(buffer);
			
			UUID rID = new UUID(in.readLong(), in.readLong());
			
			long timestamp = in.readLong();
			
			len = in.readInt();
			buffer = new byte[len];
			in.readBytes(buffer);
			String logEntry = new String(buffer);
			
			return new AppendToLog(cID, rID, logEntry, timestamp);
		}
	};
	
	public String getClientID() {
		return clientID;
	}

	public UUID getRequestID() {
		return requestID;
	}

	public String getLogEntry() {
		return logEntry;
	}

	public long getTimestamp() {
		return timestamp;
	}
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return AppendToLog.serializer;
	}
	
	public String generateFullLogEntry() {
		return "[" + clientID + "||" + timestamp + "] " + logEntry;
	}

	public byte[] serializeRequest() throws IOException {
		ByteBuf buf = Unpooled.buffer();
		AppendToLog.serializer.serialize(this, buf);
		buf.writeInt(this.signature.length);
		buf.writeBytes(this.signature);
		buf.resetReaderIndex();
		byte[] ret = new byte[buf.readableBytes()];
		buf.readBytes(ret);
		return ret;
	}
	
	public static AppendToLog deserializeRequest(byte[] buffer) throws IOException {
		ByteBuf buf = Unpooled.wrappedBuffer(buffer);
		AppendToLog request = AppendToLog.serializer.deserialize(buf);
		byte[] signature = new byte[buf.readInt()];
		buf.readBytes(signature);
		request.signature = signature;
		return request;	
	}
}
