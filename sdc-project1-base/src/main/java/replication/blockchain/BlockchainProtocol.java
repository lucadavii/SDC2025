package replication.blockchain;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.UUID;
import java.util.Arrays;


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
	private final HashMap<String, UUID> blockByHash = new HashMap<>(); //mapping from block hash to block ID
	private final HashMap<String, List<UUID>> children = new HashMap<>(); //children of each block, used for forks
	private final HashMap<String, List<ProposeBlock>> pendingChildren = new HashMap<>(); //blocks whose parent is not known yet

	private UUID head; //ID of the current head block
	private byte[] headHash; //hash of the current head block

    private long committedIndex = -1; //index of the highest committed block
    private static final int BLOCK_COMMIT_DEPTH = 6; //number of blocks after which a block is considered committed
	private static final int BATCH_SIZE = 5;
	private static final byte[] GENESIS_PREV_HASH = new byte[0];
    public BlockchainProtocol() {
		super(PROTO_NAME, PROTO_ID);
	}
	
	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		// Initialize any necessary data structures or state
		this.head = null;
		this.headHash = null;
		this.committedIndex = -1;

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
		this.replicas.put(this.self, this.myPublicKey);
	}


	public void handleAppendRequest(AppendRequest request, short protoID) {
		try{

			//store the request as pending
			this.pendingRequests.put(request.getRequestID(), request);
			if(this.pendingRequests.size() >= BATCH_SIZE) {

				long newIndex = (this.head==null) ? 0 : this.blocks.get(this.head).getIndex() + 1;
				byte[] previousHash = (this.head==null) ? GENESIS_PREV_HASH : this.headHash;
					
				//create a new block including the client request
				List<byte[]> transactions = new ArrayList<>();
				List<UUID> toRemove = new ArrayList<>();

				for(AppendRequest ar : this.pendingRequests.values()) {
					transactions.add(ar.encode());
					toRemove.add(ar.getRequestID());
				}

				ProposeBlock pb = new ProposeBlock(UUID.randomUUID(), previousHash, newIndex, this.self, transactions);
				pb.signMessage(this.myPrivateKey);
				//send the block proposal via the underlying broadcast protocol
				logger.info("Created new block proposal {}, sending via broadcast", pb.getBlockId());

				ByteBuf buf = Unpooled.buffer();
				ProposeBlock.serializer.serialize(pb, buf);
				byte[] payload = new byte[buf.readableBytes()];
				buf.readBytes(payload);

				BroadcastRequest br = new BroadcastRequest(this.self, payload, this.myPrivateKey);
				sendRequest(br, BROADCAST_PROTO_ID);
				//integrate locally
				integrateBlock(pb);
				//check if any block can be committed
				commitFromHead();
				//clear pending requests
				for(UUID reqID : toRemove) {
					this.pendingRequests.remove(reqID);
				}
			}
		}catch(Exception e){
			logger.error("Error sending BroadcastRequest to underlying protocol", e);
		}
	}
	private void integrateBlock(ProposeBlock pb) throws IOException, NoSuchAlgorithmException {
		// Integrate the proposed block into the blockchain
		if(this.blocks.containsKey(pb.getBlockId())) {
			//block already known
			return;
		}
		byte[] pbHash = ProposeBlock.hashBlock(pb);
		String pbHashStr = Arrays.toString(pbHash);
		this.blocks.put(pb.getBlockId(), pb);
		this.blockByHash.put(pbHashStr, pb.getBlockId());
		String preString = Arrays.toString(pb.getPreviousBlockHash());
		if(!this.blockByHash.containsKey(preString) && !preString.equals(Arrays.toString(GENESIS_PREV_HASH))) {
			//parent block not known yet, store as pending child indexed by parent hash
			this.pendingChildren.computeIfAbsent(preString, k-> new ArrayList<>()).add(pb);
			logger.info("Stored block {} as pending child, parent hash not known yet", pb.getBlockId());
			return;
		}
		this.children.computeIfAbsent(preString, k-> new ArrayList<>()).add(pb.getBlockId());
		//check if any pending children can now be integrated
		if(this.pendingChildren.containsKey(pbHashStr)) {
			List<ProposeBlock> pChildren = this.pendingChildren.get(pbHashStr);
			for(ProposeBlock child : pChildren) {
				integrateBlock(child);
			}
			this.pendingChildren.remove(pbHashStr);
		}
		updateHead(pb,pbHash);
	}

	private void updateHead(ProposeBlock pb, byte[] pbHash) {
		// Update the head of the blockchain if the new block has a higher index
		if(this.head==null) {
			this.head = pb.getBlockId();
			this.headHash = pbHash;
			logger.info("Updated head to block {} (index {})", pb.getBlockId(), pb.getIndex());
			return;
		}
		ProposeBlock currentHead = this.blocks.get(this.head);
		//order by index, break ties by block ID to ensure deterministic choice
		if(pb.getIndex() > currentHead.getIndex()|| (pb.getIndex() == currentHead.getIndex() && pb.getBlockId().compareTo(currentHead.getBlockId()) < 0)) {
			this.head = pb.getBlockId();
			this.headHash = pbHash;
			logger.info("Updated head to block {} (index {})", pb.getBlockId(), pb.getIndex());
		}
	}
	private UUID getAncestor(UUID blockID, long targetIndex) {
		ProposeBlock block = this.blocks.get(blockID);
		if(block.getIndex() == targetIndex) {
			return blockID;
		}
		else if(block.getIndex() < targetIndex) {
			return null; //no ancestor at that index
		}
		else {
			String parentHash = Arrays.toString(block.getPreviousBlockHash());
			UUID parentID = this.blockByHash.get(parentHash);
			return getAncestor(parentID, targetIndex);
		}
	}
	private void commitFromHead() {
		if(this.head==null) {
			return;
		}
		ProposeBlock headBlock = this.blocks.get(this.head);
		long headIndex = headBlock.getIndex();

		long committableIndex = headIndex - BLOCK_COMMIT_DEPTH;
		if(committableIndex > this.committedIndex) {
			for(long idx = this.committedIndex + 1; idx <= committableIndex; idx++) {
				UUID blockID = getAncestor(this.head, idx);
				if(blockID == null) { break;}
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
				this.committedIndex = idx;
			}
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
			integrateBlock(pb);
			logger.info("Stored new block proposal {}, index {}, proposer {}", pb.getBlockId(), pb.getIndex(), pb.getProposer());
			//check if any block can be committed
			commitFromHead();
		}catch(Exception e){
			logger.error("Error processing delivered Broadcast message", e);
		}
	}
	
}
