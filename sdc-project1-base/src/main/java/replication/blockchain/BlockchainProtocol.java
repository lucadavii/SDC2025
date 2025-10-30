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
    
    private final HashMap<byte[], ProposeBlock> blocks = new HashMap<>(); //blocks by their hash
    private final HashMap<byte[], List<byte[]>> children = new HashMap<>(); //children of each block
    private byte[] head; //hash of the current head block

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
			

            logger.info("Sending Broadcast request to underlying protocol");
			BroadcastRequest req = new BroadcastRequest(self, request.encode(), myPrivateKey);
			sendRequest(req, BROADCAST_PROTO_ID);
		}catch(Exception e){
			logger.error("Error sending BroadcastRequest to underlying protocol", e);
		}
	}

	// when the dissemination protocol responds with a notification, trigger a notification for the DLM
	public void uponBroadcastDeliver(DeliveryNotification notification, short sourceProto) {
		//Just trigger the notification to the DLM
		try{
			logger.info("Received Broadcast delivery notification, delivering to upper layer");
			AppendRequest ar = AppendRequest.decode(notification.getPayload());
			triggerNotification(new DeliverEntryNotification(ar.getRequestID(), ar.getClientID(), ar.getClientRequest(), ar.getServerSignature()));
		}catch(Exception e){
			logger.error("Error decoding AppendRequest from broadcast payload", e);
		}
		//Trigger the notification to the DLM
	}
}
