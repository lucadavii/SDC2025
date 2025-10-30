package replication.blockchain;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Properties;

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

	public void handleAppendRequest(AppendRequest request, short protoID) {
		try{
			//verify the request signature, not sure that check is correct 
			if(!request.checkSignature(this.replicas.get(request.getSource()))){
				logger.error("AppendRequest signature verification failed, dropping request");
				return;
			}
			//store the request as pending
			this.pendingRequests.put(request.getRequestID(), request);
			boolean _isMyRound = true; //replace with round robin check, otherwise everyone proposes and manage forks
			if(_isMyRound){
				//create a new block including the client request
				List<byte[]> transactions = List.of(request.getClientRequest());
				ProposeBlock pb = new ProposeBlock(UUID.randomUUID(), this.headHash, this.committedIndex + 1, this.round, this.self, transactions);
				pb.sign(this.myPrivateKey);
				//send the block proposal via the underlying broadcast protocol
				logger.info("Created new block proposal {}, sending via broadcast", pb.getBlockId());
				BroadcastRequest br = new BroadcastRequest(this.self, pb.encode(), this.myPrivateKey);
				sendRequest(br, BROADCAST_PROTO_ID);
			}
		}catch(Exception e){
			logger.error("Error sending BroadcastRequest to underlying protocol", e);
		}
	}

	public void uponBroadcastDeliver(DeliveryNotification notification, short sourceProto) {
		try{
			notification.checkSignature(replicas.get(notification.getOriginalSender()), "SHA256withRSA");
			//decode the delivered message as a ProposeBlock
			ProposeBlock pb = ProposeBlock.decode(notification.getPayload());
			//verify the block signature
			if(!pb.verifySignature(this.replicas.get(pb.getProposer()))){
				logger.error("ProposeBlock signature verification failed, dropping message");
				return;
			}
			if(!pb.getPreviousBlockHash().equals(ProposeBlock.hashBlock(this.blocks.get(this.head)))){
				logger.error("ProposeBlock previous hash does not match current head, dropping message");
				return;
			}
			if(pb.getIndex()!= this.blocks.get(head).getIndex() + 1){
				logger.error("ProposeBlock index incorrect, dropping message");
				return;
			}
			//store the block
			this.blocks.put(pb.getBlockId(), pb);
			//update head
			this.head = pb.getBlockId();
			this.headHash = ProposeBlock.hashBlock(pb);
			logger.info("New block {} added to the blockchain at index {}", pb.getBlockId(), pb.getIndex());
			//check if any block can be committed, or should be done in another method?
			


		}catch(Exception e){
			logger.error("Error processing delivered Broadcast message", e);
		}
	}
}
