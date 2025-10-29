package replication;

import java.io.IOException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Properties;

import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import replication.notifications.DeliverEntryNotification;
import replication.requests.AppendRequest;
//import broadcast.CrashFaultReliableBroadcastProtocol;
import broadcast.byzantinereliablebcast.ByzantineReliableBroadcastProtocol;
import broadcast.notifications.DeliveryNotification;
import broadcast.requests.BroadcastRequest;
import common.events.ChannelAvailable;
import pt.unl.fct.di.novasys.network.data.Host;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.HashMap;
public class ReplicationProtocolPlaceholder extends GenericProtocol {

	public final static short PROTO_ID = 900;
	public final static String PROTO_NAME = "FakeReplication";
	private final static short BROADCAST_PROTO_ID = ByzantineReliableBroadcastProtocol.PROTO_ID;

	private static final Logger logger = LogManager.getLogger(ReplicationProtocolPlaceholder.class);
	private Host self;
	private PrivateKey myPrivateKey;

	private PublicKey publicKey;
	//Map of public keys of other replicas
	private final HashMap<Host, PublicKey> publicKeys = new HashMap<>();
	public ReplicationProtocolPlaceholder() {
		super(PROTO_NAME, PROTO_ID);
	}
	
	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);
		subscribeNotification(DeliveryNotification.NOTIFICATION_ID, this::uponBroadcastDeliver);
		subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::uponChannelAvailable);

		// String h = props.getProperty("membership.myhost");
		// String[] add = h.trim().split(":");
		// this.self = new Host(InetAddress.getByName(add[0]), Integer.parseInt(add[1]));
		logger.info("Initialized Replication Protocol Placeholder at {}", self);
	}

	public void uponChannelAvailable(ChannelAvailable notification, short sourceProto) {
		
		this.self=notification.getMyHost();
		this.myPrivateKey = notification.getMyPrivateKey();
			
		this.publicKey = notification.getMyPublicKey();

		this.publicKeys.put(this.self, publicKey);

		logger.info("Channel available {}", this.self);
	}

	public void handleAppendRequest(AppendRequest request, short protoID) {
		
		//This logic is evidently incorrect. It should call the broadcast module and request diffusion
		//Should call the broadcast module and make the appen request
		//triggerNotification(new DeliverEntryNotification(request.getRequestID(), request.getClientID(), request.getClientRequest(), request.getServerSignature()));
		try{
			logger.info("Sending Broadcast request to underlying protocol");
			BroadcastRequest req = new BroadcastRequest(self, request.encode(), myPrivateKey);
       		//And send it to the broadcast protocol
       		//sendRequest(request, CrashFaultReliableBroadcastProtocol.PROTO_ID);
        	//sendRequest(request, ByzantineConsistentBroadcastProtocol.PROTO_ID);
	
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
/* testing: do not reserve machines if not necessary, people need the cluster to do research
 *  prototypes will be run in realistic environment with different machines, could experience latency between replicas
 *  rely only on the command line
 *  read documentation on how to reserve machines in the cluster
 *  each line is a physical machine, a colored line means it is occupied, red is dead machine, gray  permanent failure (need hw replacement)
 *  blue under maintanence or reserved for special purpose
 * all machines with same pokemon name have similar resources (subcluster),
 * access using ssh with custom port 12034, you get connected to frontend which has no computation power (only entry point, will die if code runs)
 * reserve computational node and move to computational node, 
 *  
 * to transfer code use scp, distributed file system, code is available for whole cluster, to upload files use scp
 * scp -r -P 12034 folder_to_copy username@cluster.di.fct.unl.pt:. 
 * if you're running multiple machines, all files will be visible to all machines, multiple writes on same files will not work
 * and consume tme and bandwidth. For logs write to /tmp directory, which exists only in the machine and gets deleted on restart/reservation end
 * transfer from tmp to computer can take a lot of time
 * 
 * to reserve machine use in frontedn oarsub -l  {"cluster='pokemon_name'} /nodes=5,walltime=2:00:00 "sleep 1d" -> doesn't terminate at end of command
 * -l allows to set options, can identify the cluester, number of nodes, walltime is amount of time in reservation, max 24h, when reservation is active you can access machine, must run a command at whose end the machine is not longer reserved
 *  cannot access computational mode if not reservation actives
 * to access machine use ssh name_of_node, internally default port is used. set keys for authentication
 * can try to put command in background, but at terminal exit process is killed
 * better idea is using tmux and ssh into the machine again after detaching
 * avoid leaving tmux instances alive, ssh connections idle for some minutes the firewall kills the connection, tmux avoids this
 * 
 * to terminate a reservation use oradel numberofjob from frontend, can be checked on diagram online if forgotten. Not instant
 * oar has a timeout for termination, after that the machine is in suspicious mode (red) and nobody can use it
 *  
 * 
 * report:
 * write as a paper, latex template will be provided
 * check slides
 * for related work avoid shopping list of technologies
 * in implementation proof of correctness is required
 * use images
 * explain results
 * prof recognizes immediately chatgpt
 * assume reader is dumb, define terminology to avoid disconnection in meaning. Bolds use for key-terms and be consistent
 * 
 * dig gives address from a name
 */
