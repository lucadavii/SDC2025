package app.messages.client;

import java.io.IOException;
import java.util.UUID;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class ReadLogEntryRequest extends SignedProtoMessage {

	public final static short MESSAGE_ID = 602;
	
	private final String clientID;
	private final UUID requestID;
	private final long entryLogIndex;
	
	public ReadLogEntryRequest(String clientID, long entryLogIndex) {
		super(MESSAGE_ID);
		this.clientID = clientID;
		this.requestID = UUID.randomUUID();
		this.entryLogIndex = entryLogIndex;
		
	}

	public ReadLogEntryRequest(String clientID, UUID requestID, long entryLogIndex) {
		super(MESSAGE_ID);
		this.clientID = clientID;
		this.requestID = requestID;
		this.entryLogIndex = entryLogIndex;
		
	}
	
	public static final SignedMessageSerializer<ReadLogEntryRequest> serializer = new SignedMessageSerializer<ReadLogEntryRequest>() {
		
		@Override
		public void serializeBody(ReadLogEntryRequest msg, ByteBuf out) throws IOException {
			out.writeInt(msg.clientID.length());
			out.writeBytes(msg.clientID.getBytes());
			
			out.writeLong(msg.requestID.getMostSignificantBits());
			out.writeLong(msg.requestID.getLeastSignificantBits());
			
			out.writeLong(msg.entryLogIndex);
		}
		
		@Override
		public ReadLogEntryRequest deserializeBody(ByteBuf in) throws IOException {
			int len = in.readInt();
			byte[] buf = new byte[len];
			in.readBytes(buf);
			String cID = new String(buf);
			
			UUID rID = new UUID(in.readLong(), in.readLong());
			
			return new ReadLogEntryRequest(cID, rID, in.readLong());
		}
	};
	
	public String getClientID() {
		return clientID;
	}



	public UUID getRequestID() {
		return requestID;
	}



	public long getEntryLogIndex() {
		return entryLogIndex;
	}



	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return ReadLogEntryRequest.serializer;
	}

}
