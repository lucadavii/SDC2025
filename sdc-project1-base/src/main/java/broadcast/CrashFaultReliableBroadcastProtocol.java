package broadcast;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import broadcast.notifications.DeliveryNotification;
import broadcast.requests.BroadcastRequest;
import broadcast.messages.BroadcastMessage;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class CrashFaultReliableBroadcastProtocol extends GenericProtocol {

	public static final String PROTO_NAME = "BestEffortBroadcast";
	public static final short PROTO_ID = 300;
	
	private HashSet<UUID> delivered;
	private HashSet<Host> neighbors;
	private HashMap<Host, PublicKey> publicKeys;
	
	private Host mySelf;
	private PublicKey myPublicKey;
	private PrivateKey myPrivateKey;
	
	private int channelID;
	
	private final Logger logger = LogManager.getLogger(CrashFaultReliableBroadcastProtocol.class);
	
	public CrashFaultReliableBroadcastProtocol() {
		super(PROTO_NAME, PROTO_ID);
		
		delivered = new HashSet<UUID>();
		neighbors = new HashSet<Host>();
		publicKeys = new HashMap<Host, PublicKey>();
		
		this.mySelf = null;
		this.myPublicKey = null;
		this.myPrivateKey = null;
	}

	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
		subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUpNotification);
		subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDownNotification);
		
		registerRequestHandler(BroadcastRequest.REQUEST_ID, this::handleBroadcastRequest);
	}
	
	public void handleChannelAvailableNotification(ChannelAvailable notification, short sourceProto) {
		this.mySelf = notification.getMyHost();
		this.myPublicKey = notification.getMyPublicKey();
		this.myPrivateKey = notification.getMyPrivateKey();
				
		this.publicKeys.put(mySelf, myPublicKey);
		
		this.channelID = notification.getChannelID();
		
		registerSharedChannel(this.channelID);
		setDefaultChannel(this.channelID);
		
		//Register Message Serializers
		registerMessageSerializer(channelID, BroadcastMessage.MESSAGE_ID, BroadcastMessage.serializer);
		
		//Setup Message Handlers
		try {
			registerMessageHandler(channelID, BroadcastMessage.MESSAGE_ID, this::uponReceiveBroadcastMessage);
		} catch (HandlerRegistrationException e) {
			//This should never happen
			e.printStackTrace();
		}
	}
	
	public void handleBroadcastRequest(BroadcastRequest req, short sourceProto) {
		
		try {
			BroadcastMessage bm = new BroadcastMessage(mySelf, req.encode());
			bm.signMessage(myPrivateKey);
			for(Host h: neighbors) {
				sendMessage(bm, h);
			}
			
			deliverMessage(bm, mySelf);
			
		} catch (Exception e) {
			//Should not happen
			e.printStackTrace();
		}
	}
	
	public void uponReceiveBroadcastMessage(BroadcastMessage msg, Host sender, short protoID, int channel) {
		if(deliverMessage(msg, sender)) {
			//This is the first time I see this message and as such I will send it to everyone
			msg.setSender(mySelf);
			try {
				msg.signMessage(myPrivateKey);
				for(Host h: this.neighbors) {
					sendMessage(msg, h);
				}
			} catch (Exception e) {
				//Should not happen
				logger.error("Could not sign the message for retransmission.");
				e.printStackTrace();
			}
		}
	}
	
	private boolean deliverMessage(BroadcastMessage msg, Host sender) {
		if(this.publicKeys.containsKey(msg.getSender())) {
			try {
				if(msg.checkSignature(this.publicKeys.get(msg.getSender()))) {
					if(!this.delivered.contains(msg.getMessageID())) {
						this.delivered.add(msg.getMessageID());
						triggerNotification(DeliveryNotification.fromMessage(msg.getPayload()));
						return true;
					}
				}
			} catch (Exception e) {
				logger.error("Could not verify authenticity of BroadcastMessage sent by " + msg.getSender());
				e.printStackTrace();
			}
		} else {
			logger.warn("I do not know the public key of " + msg.getSender() + " to validate the message.");
		}
		return false;
	}
	
	public void uponNeighborUpNotification(NeighborUp notification, short sourceProtoID) {
		logger.info("Received NeighborUp notification for: " + notification.getNeighbor());
		this.neighbors.add(notification.getNeighbor());
		this.publicKeys.put(notification.getNeighbor(), notification.getPublicKey());
	}
	
	public void uponNeighborDownNotification(NeighborDown notification, short sourceProtoID) {
		logger.info("Received NeighborDown notification for: " + notification.getNeighbor());
		this.neighbors.remove(notification.getNeighbor());
	}
}
