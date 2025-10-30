package replication.blockchain;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Properties;
import io.netty.buffer.ByteBuf;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import replication.blockchain.messages.ProposeBlock;
import replication.notifications.DeliverEntryNotification;
import replication.requests.AppendRequest;
import broadcast.byzantinereliablebcast.ByzantineReliableBroadcastProtocol;
import broadcast.notifications.DeliveryNotification;
import broadcast.requests.BroadcastRequest;
import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import io.netty.buffer.Unpooled;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;


public class BlockchainProtocol extends GenericProtocol {

	public final static short PROTO_ID = 900;
	public final static String PROTO_NAME = "BlockchainProtocol";
	private final static short BROADCAST_PROTO_ID = ByzantineReliableBroadcastProtocol.PROTO_ID;

	private static final Logger logger = LogManager.getLogger(BlockchainProtocol.class);
	
    private Host self;
	private PrivateKey myPrivateKey;
	private PublicKey myPublicKey;
	private final HashMap<Host, PublicKey> replicas = new HashMap<>();
	
    private final HashMap<UUID, AppendRequest> pendingRequests = new HashMap<>(); //pending client requests
    
    private final HashMap<UUID, ProposeBlock> blocks = new HashMap<>(); //blocks by their ID
	private final HashMap<Long, UUID> indexToBlockID = new HashMap<>(); //mapping from block index to block ID

    //private final HashMap<byte[], List<byte[]>> children = new HashMap<>(); //children of each block, used for forks
    private UUID head; //ID of the current head block
	private byte[] headHash; //hash of the current head block

    private long committedIndex = -1; //index of the highest committed block
    private static final int BLOCK_COMMIT_DEPTH = 2; //number of blocks after which a block is considered committed

    private long round = 0; //current round number

    public BlockchainProtocol() {
		super(PROTO_NAME, PROTO_ID);
	}
	
	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);
		subscribeNotification(DeliveryNotification.NOTIFICATION_ID, this::uponBroadcastDeliver);
		subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::uponChannelAvailable);
        subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
        subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);

		logger.info("Initialized Blockchain Protocol at {}", self);
	}

    public void uponNeighborUpNotification(NeighborUp notification, short sourceProtoID) {
		logger.info("Received NeighborUp notification for: " + notification.getNeighbor());
		Host neighbour = notification.getNeighbor();
		if(!this.replicas.containsKey(neighbour)) {
			this.replicas.put(neighbour, notification.getPublicKey());
		}
	}
	
	public void uponNeighborDownNotification(NeighborDown notification, short sourceProtoID) {
		logger.info("Received NeighborDown notification for: " + notification.getNeighbor());
		this.replicas.remove(notification.getNeighbor());
	}

	public void uponChannelAvailable(ChannelAvailable notification, short sourceProto) {
		
		this.self=notification.getMyHost();
		this.myPrivateKey = notification.getMyPrivateKey();			
		this.myPublicKey = notification.getMyPublicKey();
		logger.info("Channel available {}", this.self);
	}

	private boolean isMyRound(long round) {
		//simple round robin based on the order of replicas in the map
		List<Host> replicaList = List.copyOf(this.replicas.keySet());
		if (this.self!=null && !replicaList.contains(this.self)) {
			replicaList.add(this.self);
		}
		replicaList.sort((h1, h2) -> h1.toString().compareTo(h2.toString()));
		int proposerIndex = (int)(round % replicaList.size());
		return this.self!=null&&replicaList.get(proposerIndex).equals(this.self);
	}

	public void handleAppendRequest(AppendRequest request, short protoID) {
		try{

			//store the request as pending
			this.pendingRequests.put(request.getRequestID(), request);
			if(isMyRound(this.round)){
				long newIndex = (this.head==null) ? 0 : this.blocks.get(this.head).getIndex() + 1;
				byte[] previousHash = (this.head==null) ? new byte[32] : this.headHash;
					
				//create a new block including the client request
				List<byte[]> transactions = List.of(request.getClientRequest());
				ProposeBlock pb = new ProposeBlock(UUID.randomUUID(), previousHash, newIndex, this.round, this.self, transactions);
				pb.signMessage(this.myPrivateKey);
				//send the block proposal via the underlying broadcast protocol
				logger.info("Created new block proposal {}, sending via broadcast", pb.getBlockId());

				ByteBuf buf = Unpooled.buffer();
				ProposeBlock.serializer.serialize(pb, buf);
				byte[] payload = new byte[buf.readableBytes()];
				buf.readBytes(payload);

				BroadcastRequest br = new BroadcastRequest(this.self, payload, this.myPrivateKey);
				sendRequest(br, BROADCAST_PROTO_ID);
			}
		}catch(Exception e){
			logger.error("Error sending BroadcastRequest to underlying protocol", e);
		}
	}

	public void uponBroadcastDeliver(DeliveryNotification notification, short sourceProto) {
		try{
			byte[] payload = notification.getPayload();
			ByteBuf buf = Unpooled.wrappedBuffer(payload);
			ProposeBlock pb = ProposeBlock.serializer.deserialize(buf);

			PublicKey proposerKey = this.replicas.get(pb.getProposer());
			if(proposerKey == null) {
				logger.warn("Unknown proposer {}, ignoring block proposal {}", pb.getProposer(), pb.getBlockId());
				return;
			}
			if(!pb.checkSignature(proposerKey)) {
				logger.warn("Invalid signature for block proposal {}, ignoring", pb.getBlockId());
				return;
			}
			//store the block
			this.blocks.put(pb.getBlockId(), pb);
			this.indexToBlockID.put(pb.getIndex(), pb.getBlockId());
			this.head = pb.getBlockId();
			this.headHash = ProposeBlock.hashBlock(pb);
			logger.info("Stored new block proposal {}, index {}, round {}, proposer {}", pb.getBlockId(), pb.getIndex(), pb.getRound(), pb.getProposer());
			//check if any block can be committed
			commitBlocks();
			this.round += 1; //advance to the next round
		}catch(Exception e){
			logger.error("Error processing delivered Broadcast message", e);
		}
	}
	private void commitBlocks(){
		long candidateIndex = this.committedIndex + 1;
		long headIndex = this.blocks.get(this.head).getIndex();
		while(candidateIndex + BLOCK_COMMIT_DEPTH <= headIndex){
			UUID blockID = this.indexToBlockID.get(candidateIndex);
			ProposeBlock block = this.blocks.get(blockID);
			//deliver all client requests in the block
			for(byte[] clientRequestBytes : block.getTransactions()){
				try{
					AppendRequest ar = AppendRequest.decode(clientRequestBytes);
					//create and send DeliverEntryNotification
					DeliverEntryNotification den = new DeliverEntryNotification(ar.getRequestID(), ar.getClientID(), ar.getClientRequest(), ar.getServerSignature());
					den.sign(myPrivateKey);
					triggerNotification(den);
					logger.info("Committed block {}, delivered client request {}", blockID, ar.getRequestID());
				}catch(Exception e){
					logger.error("Error decoding client request in block {}", blockID, e);
				}
			}
			this.committedIndex = candidateIndex;
			candidateIndex += 1;
		}
	}
}
