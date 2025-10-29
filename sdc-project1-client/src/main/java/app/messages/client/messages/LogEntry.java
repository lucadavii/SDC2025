package app.messages.client.messages;

import java.io.IOException;
import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;

public class LogEntry extends SignedProtoMessage {

	public final static short MESSAGE_ID = 606;
	
	private final long index;
	private final String logEntry;

	
	public LogEntry(long index, String logEntry) {
		super(MESSAGE_ID);
		this.index = index;
		this.logEntry = logEntry;
	}
	
	public static SignedMessageSerializer<LogEntry> serializer = new SignedMessageSerializer<LogEntry>() {
		
		@Override
		public void serializeBody(LogEntry msg, ByteBuf out) throws IOException {			
			out.writeLong(msg.index);
			
			out.writeInt(msg.logEntry.length());
			out.writeBytes(msg.logEntry.getBytes());
		}
		
		@Override
		public LogEntry deserializeBody(ByteBuf in) throws IOException {
			int len = in.readInt();
			
			byte[] buffer = new byte[len];
			
			long index = in.readLong();
			
			len = in.readInt();
			buffer = new byte[len];
			in.readBytes(buffer);
			String logEntry = new String(buffer);
			
			return new LogEntry(index, logEntry);
		}
	};

	public String getLogEntry() {
		return logEntry;
	}

	public long getIndex() {
		return index;
	}
	
	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return LogEntry.serializer;
	}
	
	public String toString() {
		return "[" + index + "] " + logEntry;
	}

}
