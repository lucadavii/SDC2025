package replication.blockchain.messages;
import java.util.UUID;
import java.util.List;
import java.util.ArrayList;

import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedMessageSerializer;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;

import utils.SignaturesHelper;

public class ProposeBlock extends SignedProtoMessage{
    public final static short MSG_ID = 901;

    private UUID blockId;
    private byte[] previousBlockHash;
    private long index;
    private long round;
    private Host proposer;
    private List<byte[]> transactions;
    private byte[] signature;

    public ProposeBlock(UUID blockID, byte[] previousBlockHash, long index, long round, Host proposer, List<byte[]> transactions) {
        super(ProposeBlock.MSG_ID);
        this.blockId = blockID;
        this.previousBlockHash = previousBlockHash;
        this.index = index;
        this.round = round;
        this.proposer = proposer;
        this.transactions = transactions;
        this.signature = null;
    }
    public UUID getBlockId() {
        return blockId;
    }
    public byte[] getPreviousBlockHash() {
        return previousBlockHash;
    }
    public long getIndex() {
        return index;
    }
    public long getRound() {
        return round;
    }
    public Host getProposer() {
        return proposer;
    }
    public List<byte[]> getTransactions() {
        return transactions;
    }
    public byte[] getSignature() {
        return signature;
    }

    public void sign(PrivateKey key) throws InvalidKeyException, NoSuchAlgorithmException,SignatureException, IOException {
        ByteBuf buf = Unpooled.buffer();
        serializer.serializeBody(this, buf);
        buf.resetReaderIndex();
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        this.signature = SignaturesHelper.generateSignature(payload, key);      
    }

    public boolean verifySignature(PublicKey key)  throws SignatureException, NoSuchAlgorithmException, InvalidKeyException, IOException {
        ByteBuf buf = Unpooled.buffer();
        serializer.serializeBody(this, buf);
        buf.resetReaderIndex();
        byte[] payload = new byte[buf.readableBytes()];
        buf.readBytes(payload);
        return SignaturesHelper.checkSignature(payload, this.signature, key);
    }

    public byte[] encode() throws IOException {
        ByteBuf buf = Unpooled.buffer();
        serializer.serialize(this, buf);
        if(this.signature != null) {
            buf.writeInt(this.signature.length);
            buf.writeBytes(this.signature);
        }
        else {
            buf.writeInt(0);
        }

        buf.resetReaderIndex();
        byte[] encoded = new byte[buf.readableBytes()];
        buf.readBytes(encoded);
        return encoded;
    }
    public static ProposeBlock decode(byte[] data)  throws IOException {
        ByteBuf buf = Unpooled.wrappedBuffer(data);
        ProposeBlock msg = serializer.deserialize(buf);
        int sigLength = buf.readInt();
        if (sigLength > 0) {
            byte[] signature = new byte[sigLength];
            buf.readBytes(signature);
            msg.signature = signature;
        }
        return msg;
    }

    public final static SignedMessageSerializer<ProposeBlock> serializer = new SignedMessageSerializer<ProposeBlock>() {
        @Override
        public void serializeBody(ProposeBlock msg, ByteBuf out) throws IOException {
            Host.serializer.serialize(msg.proposer, out);
            out.writeLong(msg.blockId.getMostSignificantBits());
            out.writeLong(msg.blockId.getLeastSignificantBits());
            out.writeInt(msg.previousBlockHash.length);
            out.writeBytes(msg.previousBlockHash);
            out.writeLong(msg.index);
            out.writeLong(msg.round);
            if (msg.transactions != null) {
                out.writeInt(msg.transactions.size());
                for (byte[] tx : msg.transactions) {
                    out.writeInt(tx.length);
                    out.writeBytes(tx);
                }
            }
            else {
                out.writeInt(0);
            }

        }

        @Override
        public ProposeBlock deserializeBody(ByteBuf in) throws IOException {
            Host proposer = Host.serializer.deserialize(in);
            UUID blockId = new UUID(in.readLong(), in.readLong());
            byte[] previousBlockHash = new byte[in.readInt()];
            in.readBytes(previousBlockHash);
            long index = in.readLong();
            long round = in.readLong();
            List<byte[]> transactions = new ArrayList<>();
            int txCount = in.readInt();
            for (int i = 0; i < txCount; i++) {
                byte[] tx = new byte[in.readInt()];
                in.readBytes(tx);
                transactions.add(tx);
            }
            return new ProposeBlock(blockId, previousBlockHash, index, round, proposer, transactions);
        }
    };


    @Override
    public SignedMessageSerializer<? extends SignedProtoMessage> getSerializer() {
        return serializer;
    }


}