package app.messages.client.messages;

import java.io.IOException;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class ReadLogEntryReply extends SignedProtoMessage {

	public final static short MESSAGE_ID = 605;
	
	private final long entryLogIndex;
	private final String logEntry;
	
	public ReadLogEntryReply(long entryLogIndex, String logEntry) {
		super(MESSAGE_ID);
		this.entryLogIndex = entryLogIndex;
		this.logEntry = logEntry;
	}
	
	public static final SignedMessageSerializer<ReadLogEntryReply> serializer = new SignedMessageSerializer<ReadLogEntryReply>() {
		
		@Override
		public void serializeBody(ReadLogEntryReply msg, ByteBuf out) throws IOException {			
			out.writeLong(msg.entryLogIndex);
			
			out.writeInt(msg.logEntry.length());
			out.writeBytes(msg.logEntry.getBytes());
		}
		
		@Override
		public ReadLogEntryReply deserializeBody(ByteBuf in) throws IOException {						
			long index = in.readLong();
			
			int len = in.readInt();
			byte[] buf = new byte[len];
			in.readBytes(buf);
			
			return new ReadLogEntryReply(index, new String(buf));
		}
	};
	
	public long getEntryLogIndex() {
		return entryLogIndex;
	}

	public String getLogEntry() {
		return logEntry;
	}
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return ReadLogEntryReply.serializer;
	}

}
