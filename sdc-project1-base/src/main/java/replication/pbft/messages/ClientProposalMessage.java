package replication.pbft.messages;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;

public class ClientProposalMessage extends SignedProtoMessage {

	public static final short MESSAGE_ID = 2001;

	private final Host sender;
	private final byte[] clientPayload;

	public ClientProposalMessage(Host sender, byte[] clientPayload) {
		super(MESSAGE_ID);
		this.sender = sender;
		this.clientPayload = clientPayload;
	}

	public Host getSender() { return sender; }
	public byte[] getClientPayload() { return clientPayload; }

	public static final SignedMessageSerializer<ClientProposalMessage> serializer = new SignedMessageSerializer<ClientProposalMessage>() {
		@Override
		public void serializeBody(ClientProposalMessage m, ByteBuf out) throws IOException {
			Host.serializer.serialize(m.sender, out);
			out.writeInt(m.clientPayload.length);
			out.writeBytes(m.clientPayload);
		}

		@Override
		public ClientProposalMessage deserializeBody(ByteBuf in) throws IOException {
			Host s = Host.serializer.deserialize(in);
			int len = in.readInt();
			byte[] pl = new byte[len];
			in.readBytes(pl);
			return new ClientProposalMessage(s, pl);
		}
	};

	@Override
	public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
		return ClientProposalMessage.serializer;
	}
}


