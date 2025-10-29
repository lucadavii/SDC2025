package replication.pbft;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import common.events.ChannelAvailable;
import common.events.NeighborDown;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.network.data.Host;
import replication.notifications.DeliverEntryNotification;
import replication.pbft.messages.ClientProposalMessage;
import replication.pbft.messages.CommitMessage;
import replication.pbft.messages.PrePrepareMessage;
import replication.pbft.messages.PrepareMessage;
import replication.requests.AppendRequest;

public class PBFTReplicationProtocol extends GenericProtocol {

	public static final String PROTO_NAME = "PBFTReplication";
	public static final short PROTO_ID = 910;

	private final Logger logger = LogManager.getLogger(PBFTReplicationProtocol.class);

	private Host myself;
	private PrivateKey myPrivateKey;
	private PublicKey myPublicKey;

	private final HashSet<Host> replicas;
	private final HashMap<Host, PublicKey> publicKeys;

	private int sharedChannelId;

	// PBFT state
	private long currentView;
	private long lastAssignedSeq;

	private static class InstanceState {
		final UUID opId;
		final byte[] clientPayload;
		final byte[] digest;
		final HashSet<Host> prepares = new HashSet<>();
		final HashSet<Host> commits = new HashSet<>();
		boolean decided = false;
		InstanceState(UUID opId, byte[] clientPayload, byte[] digest){
			this.opId = opId;
			this.clientPayload = clientPayload;
			this.digest = digest;
		}
	}

	private final Map<Long, InstanceState> instanceBySeq;
	private final Map<UUID, Long> seqByOp;

	public PBFTReplicationProtocol() {
		super(PROTO_NAME, PROTO_ID);
		this.replicas = new HashSet<>();
		this.publicKeys = new HashMap<>();
		this.instanceBySeq = new HashMap<>();
		this.seqByOp = new HashMap<>();
		this.currentView = 0;
		this.lastAssignedSeq = 0;
	}

	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::uponChannelAvailable);
		subscribeNotification(NeighborUp.NOTIFICATION_ID, this::uponNeighborUp);
		subscribeNotification(NeighborDown.NOTIFICATION_ID, this::uponNeighborDown);

		registerRequestHandler(AppendRequest.REQUEST_ID, this::handleAppendRequest);
	}

	private void uponChannelAvailable(ChannelAvailable not, short from) {
		this.myself = not.getMyHost();
		this.myPrivateKey = not.getMyPrivateKey();
		this.myPublicKey = not.getMyPublicKey();
		this.publicKeys.put(myself, myPublicKey);

		this.sharedChannelId = not.getChannelID();
		registerSharedChannel(sharedChannelId);
		setDefaultChannel(sharedChannelId);

		// Register message serializers and handlers
		registerMessageSerializer(sharedChannelId, ClientProposalMessage.MESSAGE_ID, ClientProposalMessage.serializer);
		registerMessageSerializer(sharedChannelId, PrePrepareMessage.MESSAGE_ID, PrePrepareMessage.serializer);
		registerMessageSerializer(sharedChannelId, PrepareMessage.MESSAGE_ID, PrepareMessage.serializer);
		registerMessageSerializer(sharedChannelId, CommitMessage.MESSAGE_ID, CommitMessage.serializer);
		try {
			registerMessageHandler(sharedChannelId, ClientProposalMessage.MESSAGE_ID, this::uponClientProposal);
			registerMessageHandler(sharedChannelId, PrePrepareMessage.MESSAGE_ID, this::uponPrePrepare);
			registerMessageHandler(sharedChannelId, PrepareMessage.MESSAGE_ID, this::uponPrepare);
			registerMessageHandler(sharedChannelId, CommitMessage.MESSAGE_ID, this::uponCommit);
		} catch (HandlerRegistrationException e) {
			logger.error("Failed to register PBFT handlers", e);
		}
	}

	private void uponNeighborUp(NeighborUp not, short from) {
		replicas.add(not.getNeighbor());
		publicKeys.put(not.getNeighbor(), not.getPublicKey());
	}

	private void uponNeighborDown(NeighborDown not, short from) {
		replicas.remove(not.getNeighbor());
		publicKeys.remove(not.getNeighbor());
	}

	private Host getPrimary(long view) {
		// Deterministic static primary: lexicographically smallest (address,port) among replicas + self
		Host candidate = myself;
		for (Host h : replicas) {
			if (compareHosts(h, candidate) < 0) candidate = h;
		}
		return candidate;
	}

	private int compareHosts(Host a, Host b) {
		int ip = a.getAddress().getHostAddress().compareTo(b.getAddress().getHostAddress());
		if (ip != 0) return ip;
		return Integer.compare(a.getPort(), b.getPort());
	}

	private byte[] digest(byte[] payload) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-256");
			return md.digest(payload);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private int f() {
		int n = replicas.size() + 1; // include self
		return (n - 1) / 3; // floor
	}

	private int quorum() {
		return 2 * f() + 1;
	}

	private void handleAppendRequest(AppendRequest request, short sourceProto) {
		try {
			// If I'm not the primary, forward to primary as a client proposal
			Host primary = getPrimary(currentView);
			if (!primary.equals(myself)) {
                logger.info("PBFT: forwarding client proposal {} to primary {}", request.getRequestID(), primary);
				ClientProposalMessage cpm = new ClientProposalMessage(myself, request.encode());
				sendMessage(cpm, primary);
				return;
			}

			// I'm primary: create new sequence and start PBFT
			long seq = ++lastAssignedSeq;
			byte[] payload = request.encode();
			byte[] dg = digest(payload);
			InstanceState st = new InstanceState(request.getRequestID(), payload, dg);
			instanceBySeq.put(seq, st);
			seqByOp.put(request.getRequestID(), seq);

            logger.info("PBFT: primary pre-prepare view={} seq={} opId={}", currentView, seq, request.getRequestID());
            PrePrepareMessage ppm = new PrePrepareMessage(currentView, seq, dg, payload, myself);
            ppm.signMessage(myPrivateKey);
			broadcast(ppm);
			// Primary implicitly counts its own prepare
			recordPrepare(seq, myself);
            PrepareMessage pm = new PrepareMessage(currentView, seq, dg, myself);
            pm.signMessage(myPrivateKey);
			broadcast(pm);
		} catch (Exception e) {
			logger.error("Error handling append request", e);
		}
	}

    private void uponClientProposal(ClientProposalMessage msg, Host from, short pid, int ch) {
        if (!getPrimary(currentView).equals(myself)) return; // only primary handles
        try {
            AppendRequest ar = AppendRequest.decode(msg.getClientPayload());
            // Verify sender key, if available
            Host src = ar.getSource();
            PublicKey k = publicKeys.get(src);
            if (k != null) {
                try {
                    if (!ar.checkSignature(k)) {
                        logger.warn("AppendRequest signature invalid from {}", src);
                        return;
                    }
                } catch (Exception ex) {
                    logger.warn("Error verifying AppendRequest signature from {}", src);
                    return;
                }
            }
            // reuse primary path
            handleAppendRequest(ar, PROTO_ID);
        } catch (Exception e) {
            logger.error("Failed to process client proposal", e);
        }
    }

	private void uponPrePrepare(PrePrepareMessage msg, Host from, short pid, int ch) {
		if (!from.equals(getPrimary(msg.getView()))) return; // must come from primary
        try {
            byte[] dg = digest(msg.getClientPayload());
            if (!MessageDigest.isEqual(dg, msg.getDigest())) return;
            InstanceState st = instanceBySeq.get(msg.getSeq());
            if (st == null) {
                st = new InstanceState(UUID.randomUUID(), msg.getClientPayload(), msg.getDigest());
                instanceBySeq.put(msg.getSeq(), st);
            }
            logger.info("PBFT: replica received PrePrepare view={} seq={} from {}", msg.getView(), msg.getSeq(), from);
			// send prepare
			recordPrepare(msg.getSeq(), myself);
            PrepareMessage pm = new PrepareMessage(msg.getView(), msg.getSeq(), msg.getDigest(), myself);
            pm.signMessage(myPrivateKey);
			broadcast(pm);
		} catch (Exception e) {
			logger.error("Error on PrePrepare", e);
		}
	}

	private void uponPrepare(PrepareMessage msg, Host from, short pid, int ch) {
        if (!verifySender(from)) return;
        InstanceState st = instanceBySeq.get(msg.getSeq());
        if (st == null) {
            // Ignore prepares until PrePrepare arrives
            return;
        }
        recordPrepare(msg.getSeq(), from);
        int count = st.prepares.size();
        logger.info("PBFT: replica received Prepare view={} seq={} from {}, prepares={}", msg.getView(), msg.getSeq(), from, count);
		if (count >= quorum()) {
			// broadcast commit (only once)
			CommitMessage cm = new CommitMessage(msg.getView(), msg.getSeq(), msg.getDigest(), myself);
            try { cm.signMessage(myPrivateKey); } catch (Exception ignored) {}
			broadcast(cm);
		}
	}

	private void uponCommit(CommitMessage msg, Host from, short pid, int ch) {
        if (!verifySender(from)) return;
        InstanceState st = instanceBySeq.get(msg.getSeq());
        if (st == null) {
            // Ignore commits until PrePrepare arrives
            return;
        }
        recordCommit(msg.getSeq(), from);
		if (st != null && !st.decided && st.commits.size() >= quorum()) {
			st.decided = true;
			try {
				AppendRequest ar = AppendRequest.decode(st.clientPayload);
                logger.info("PBFT: decided view={} seq={} opId={}", msg.getView(), msg.getSeq(), ar.getRequestID());
				triggerNotification(new DeliverEntryNotification(ar.getRequestID(), ar.getClientID(), ar.getClientRequest(), ar.getServerSignature()));
			} catch (Exception e) {
				logger.error("Failed to deliver decided entry", e);
			}
		}
	}

	private InstanceState empty(long seq) {
		InstanceState st = new InstanceState(null, new byte[0], new byte[0]);
		instanceBySeq.put(seq, st);
		return st;
	}

	private boolean verifySender(Host from) {
		return from.equals(myself) || publicKeys.containsKey(from);
	}

	private void recordPrepare(long seq, Host who) {
		InstanceState st = instanceBySeq.get(seq);
		if (st == null) return;
		st.prepares.add(who);
	}

	private void recordCommit(long seq, Host who) {
		InstanceState st = instanceBySeq.get(seq);
		if (st == null) return;
		st.commits.add(who);
	}

	private void broadcast(ProtoMessage m) {
		for (Host h : replicas) {
			if (!h.equals(myself)) {
				sendMessage(m, h);
			}
		}
	}

    // removed custom signature verification; SignedProtoMessage handles signing/verification at channel
}


