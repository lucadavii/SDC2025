package replication.pbft.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class CommitMessage extends SignedProtoMessage {

	public static final short MESSAGE_ID = 2004;

	private final long view;
	private final long seq;
	private final byte[] digest;
	private final Host sender;

	public CommitMessage(long view, long seq, byte[] digest, Host sender) {
		super(MESSAGE_ID);
		this.view = view;
		this.seq = seq;
		this.digest = digest;
		this.sender = sender;
	}

	public long getView() { return view; }
	public long getSeq() { return seq; }
	public byte[] getDigest() { return digest; }
	public Host getSender() { return sender; }

	public static final SignedMessageSerializer<CommitMessage> serializer = new SignedMessageSerializer<CommitMessage>() {
		@Override
		public void serializeBody(CommitMessage m, ByteBuf out) throws IOException {
			out.writeLong(m.view);
			out.writeLong(m.seq);
			out.writeInt(m.digest.length);
			out.writeBytes(m.digest);
			Host.serializer.serialize(m.sender, out);
		}

		@Override
		public CommitMessage deserializeBody(ByteBuf in) throws IOException {
			long v = in.readLong();
			long s = in.readLong();
			int dl = in.readInt();
			byte[] d = new byte[dl];
			in.readBytes(d);
			Host snd = Host.serializer.deserialize(in);
			return new CommitMessage(v, s, d, snd);
		}
	};

	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return CommitMessage.serializer;
	}
}


