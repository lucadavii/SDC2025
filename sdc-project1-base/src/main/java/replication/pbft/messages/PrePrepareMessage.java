package replication.pbft.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class PrePrepareMessage extends SignedProtoMessage {

	public static final short MESSAGE_ID = 2002;

	private final long view;
	private final long seq;
	private final byte[] digest;
	private final byte[] clientPayload;
	private final Host sender;

	public PrePrepareMessage(long view, long seq, byte[] digest, byte[] clientPayload, Host sender) {
		super(MESSAGE_ID);
		this.view = view;
		this.seq = seq;
		this.digest = digest;
		this.clientPayload = clientPayload;
		this.sender = sender;
	}

	public long getView() { return view; }
	public long getSeq() { return seq; }
	public byte[] getDigest() { return digest; }
	public byte[] getClientPayload() { return clientPayload; }
	public Host getSender() { return sender; }

	public static final SignedMessageSerializer<PrePrepareMessage> serializer = new SignedMessageSerializer<PrePrepareMessage>() {
		@Override
		public void serializeBody(PrePrepareMessage m, ByteBuf out) throws IOException {
			out.writeLong(m.view);
			out.writeLong(m.seq);
			out.writeInt(m.digest.length);
			out.writeBytes(m.digest);
			out.writeInt(m.clientPayload.length);
			out.writeBytes(m.clientPayload);
			Host.serializer.serialize(m.sender, out);
		}

		@Override
		public PrePrepareMessage deserializeBody(ByteBuf in) throws IOException {
			long v = in.readLong();
			long s = in.readLong();
			int dl = in.readInt();
			byte[] d = new byte[dl];
			in.readBytes(d);
			int pl = in.readInt();
			byte[] cp = new byte[pl];
			in.readBytes(cp);
			Host snd = Host.serializer.deserialize(in);
			return new PrePrepareMessage(v, s, d, cp, snd);
		}
	};

	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return PrePrepareMessage.serializer;
	}
}


