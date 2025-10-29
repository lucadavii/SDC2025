package broadcast.byzantinereliablebcast;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import broadcast.notifications.DeliveryNotification;
import broadcast.requests.BroadcastRequest;
import broadcast.messages.BroadcastMessage;
import broadcast.messages.EchoMessage;
import broadcast.messages.ReadyMessage;
import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.network.data.Host;

public class ByzantineReliableBroadcastProtocol extends GenericProtocol{
    public static final String PROTO_NAME = "ByzantineReliableBroadcast";
    public static final short PROTO_ID = 302;

    private HashSet<UUID> delivered;
    private HashMap<UUID,HashMap<Host,byte[]>> echos;
    private HashMap<UUID,HashMap<Host,byte[]>> readys;


    private HashSet<Host> neighbors;
    private HashMap<Host, PublicKey> publicKeys;

    private Host mySelf;
    private PublicKey myPublicKey;
    private PrivateKey myPrivateKey;

    private int channelID;

    private final Logger logger = LogManager.getLogger(ByzantineReliableBroadcastProtocol.class);

    public ByzantineReliableBroadcastProtocol() {
        super(PROTO_NAME, PROTO_ID);

        delivered = new HashSet<UUID>();
        echos = new HashMap<UUID,HashMap<Host,byte[]>>();
        readys = new HashMap<UUID,HashMap<Host,byte[]>>();
        neighbors = new HashSet<Host>();
        publicKeys = new HashMap<Host, PublicKey>();

        this.mySelf = null;
        this.myPublicKey = null;
        this.myPrivateKey = null;
    }

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

        registerMessageSerializer(channelID,BroadcastMessage.MESSAGE_ID,BroadcastMessage.serializer);
        registerMessageSerializer(channelID,EchoMessage.MESSAGE_ID,EchoMessage.serializer);
        registerMessageSerializer(channelID,ReadyMessage.MESSAGE_ID,ReadyMessage.serializer);
        try{
            registerMessageHandler(channelID, BroadcastMessage.MESSAGE_ID, this::uponReceiveBroadcastMessage);
            registerMessageHandler(channelID, EchoMessage.MESSAGE_ID, this::uponReceiveEchoMessage);
            registerMessageHandler(channelID, ReadyMessage.MESSAGE_ID,this::uponReceiveReadyMessage);
        }catch (HandlerRegistrationException e){
            e.printStackTrace();
        }
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

    public void uponReceiveBroadcastMessage(BroadcastMessage msg, Host sender, short protoID, int channel){
        //when a broadcast message is received, send echoes to everyone
        PublicKey senderKey = this.publicKeys.get(msg.getSender());
        if(senderKey == null) return;
        try {
            if(!msg.checkSignature(senderKey)) {
                logger.error("Invalid signature for BroadcastMessage from " + sender);
                return;
            }
        } catch (Exception e) {
            logger.error("Error while checking signature for BroadcastMessage from " + sender);
            e.printStackTrace();
            return;
        }
        UUID mID = msg.getMessageID();
        if (delivered.contains(mID)) return; //if I've already delivered the message, ignore it

        HashMap<Host,byte[]> map = echos.computeIfAbsent(mID,k->new HashMap<>() );
        if (!map.containsKey(mySelf)){
            map.put(mySelf,msg.getPayload()); //send echo message to self
            EchoMessage em = new EchoMessage(mySelf,mID,msg.getPayload());
            try {
                em.signMessage(myPrivateKey);
                for (Host h : neighbors) {
                    sendMessage(channelID,em,PROTO_ID,h,0);
                    logger.info("Sent EchoMessage " + mID + " to " + h);
                }
            } catch (Exception e) {
                logger.error("Error signing EchoMessage");
                e.printStackTrace();
            }
        }
    }
    public void uponReceiveEchoMessage(EchoMessage msg, Host sender, short protoID, int channel){
        //when an echo message is received, register it and if enough echoes are received, send ready messages to everyone
        PublicKey senderKey = this.publicKeys.get(msg.getSender());
        if(senderKey == null) return;
        try {
            if(!msg.checkSignature(senderKey)) {
                logger.error("Invalid signature for EchoMessage from " + sender);
                return;
            }
        } catch (Exception e) {
            logger.error("Error while checking signature for EchoMessage from " + sender);
            e.printStackTrace();
            return;
        }
        UUID mID = msg.getMessageID();
        if (delivered.contains(mID)) return; //if I've already delivered the message, ignore it

        HashMap<Host,byte[]> map = echos.computeIfAbsent(mID,k->new HashMap<>() );
        byte[] prev = map.get(msg.getSender()); //check if we already received an echo from that sender
        if (prev == null){ //if we didn't receive an echo from that sender or if the payload is different, register the echo
            map.put(msg.getSender(),msg.getPayload());
        }
        else if (!java.util.Arrays.equals(prev,msg.getPayload())){
            logger.warn("Received different EchoMessage payload from " + msg.getSender() + " for message " + mID);
            return;
        }else{
            return; //duplicate echo message, ignore
        }

        int n = neighbors.size()+1;
        int f = Math.max(0,(n-1)/3);
        int threshold = (n+f)/2;

        long matches = map.values().stream().filter(p->java.util.Arrays.equals(p,msg.getPayload())).count();

        if (matches>=threshold){ //if we received enough echoes, send ready messages
            HashMap<Host,byte[]> rmap = readys.computeIfAbsent(mID,k->new HashMap<>());
            if (!rmap.containsKey(mySelf)){ //send ready message to self
                rmap.put(mySelf,msg.getPayload());

                ReadyMessage rm = new ReadyMessage(mySelf,mID,msg.getPayload());
                try {
                    rm.signMessage(myPrivateKey);
                    for (Host h : neighbors) {
                        sendMessage(channelID,rm,PROTO_ID,h,0);
                        logger.info("Sent ReadyMessage " + mID + " to " + h);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public void uponReceiveReadyMessage(ReadyMessage msg, Host sender, short protoID, int channel){
        //when a ready message is received, register it and if enough ready messages are received, deliver the message
        PublicKey senderKey = this.publicKeys.get(msg.getSender());
        if (senderKey == null) return;
        try {
            if (!msg.checkSignature(senderKey)) {
                logger.error("Invalid signature for ReadyMessage from " + sender);
                return;
            }
        } catch (Exception e) {
            logger.error("Error while checking signature for ReadyMessage from " + sender);
            e.printStackTrace();
            return;
        }

        UUID mID = msg.getMessageID();
        if (delivered.contains(mID)) return; //if I've already delivered the message, ignore it

        HashMap<Host, byte[]> map = readys.computeIfAbsent(mID, k -> new HashMap<>());
        byte[] prev = map.get(msg.getSender()); //check if we already received a ready from that sender
        if (prev == null) { //if we didn't receive a ready from that sender or if the payload is different, register the ready
            map.put(msg.getSender(), msg.getPayload());
        }
        else if (!java.util.Arrays.equals(prev, msg.getPayload())) {
            logger.warn("Received different ReadyMessage payload from " + msg.getSender() + " for message " + mID);
            return;
        }else{
            return; //duplicate ready message, ignore
        }

        int n = neighbors.size() + 1;
        int f = Math.max(0, (n - 1) / 3);
        int threshold = f + 1;
        // if f+1 ready messages are received, propagate the ready message
        
        long matches = map.values().stream().filter(p -> java.util.Arrays.equals(p, msg.getPayload())).count();

        if (!map.containsKey(mySelf) && matches >= threshold) {
            //send ready message to self, acts as a guard
                map.put(mySelf, msg.getPayload());
                //prepare a new ready message and send to everyone
                ReadyMessage rm = new ReadyMessage(mySelf, mID, msg.getPayload());
                try {
                    rm.signMessage(myPrivateKey);
                    for (Host h : neighbors) {
                        sendMessage(channelID,rm,PROTO_ID,h,0);
                        logger.info("Sent ReadyMessage " + mID + " to " + h);
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
        }
        threshold = 2 * f + 1;
        // if 2f+1 ready messages are received, deliver the message

        matches = map.values().stream().filter(p -> java.util.Arrays.equals(p, msg.getPayload())).count();

        if (matches >= threshold) {
            try {
                triggerNotification(DeliveryNotification.fromMessage(msg.getPayload()));

            } catch (Exception e) {
                e.printStackTrace();
                logger.error("Error while delivering message " + mID);
            } finally {
                delivered.add(mID);
                logger.info("Delivered message " + mID + " with " + matches + " matching Ready messages.");
                echos.remove(mID);
                readys.remove(mID);
            }
        }
    }

    public void handleBroadcastRequest(BroadcastRequest request, short sourceProto) {
        try {
            BroadcastMessage bm = new BroadcastMessage(mySelf, request.encode());
            UUID mID = bm.getMessageID();
            bm.signMessage(myPrivateKey);
            for (Host h : neighbors) {
                sendMessage(channelID,bm,PROTO_ID,h,0);
                logger.info("Sent BroadcastMessage " + mID + " to " + h);
            }
            //technically I've sent the bc message to myself, so I register it and send echo to everyone
            HashMap<Host, byte[]> map = echos.computeIfAbsent(bm.getMessageID(), k -> new HashMap<>());
            if (!map.containsKey(mySelf)) {
                map.put(mySelf, bm.getPayload());
                EchoMessage em = new EchoMessage(mySelf, mID, bm.getPayload());
                em.signMessage(myPrivateKey);
                for (Host h : neighbors) {
                    sendMessage(channelID,em,PROTO_ID,h,0);
                    logger.info("Sent EchoMessage " + mID + " to " + h);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
