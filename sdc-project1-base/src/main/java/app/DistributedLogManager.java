package app;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app.messages.client.AppendToLog;
import app.messages.client.AppendToLogReply;
import app.messages.client.LogEntry;
import app.messages.client.ReadLogEntryReply;
import app.messages.client.ReadLogEntryRequest;
import app.messages.client.SubscribeToLog;
import common.events.ChannelAvailable;
import common.events.NeighborUp;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;
import pt.unl.fct.di.novasys.babel.generic.ProtoMessage;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleServerChannel;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.InConnectionUp;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionDown;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionFailed;
import pt.unl.fct.di.novasys.channel.tcp.events.OutConnectionUp;
import pt.unl.fct.di.novasys.network.data.Host;
import replication.notifications.DeliverEntryNotification;
import replication.requests.AppendRequest;

public class DistributedLogManager extends GenericProtocol {

	private static final Logger logger = LogManager.getLogger(DistributedLogManager.class);

	public static final String BASE_LOG_NAME = "DistributedLogManager-XXX.log";

	public static final String ADDRESS_KEY = "address";
	public static final String SERVER_PORT_KEY = "server_port";

	public final static String PROTO_NAME = "DistributedLogManager";
	public final static short PROTO_ID = 500;

	private int clientChannel;

	private final Map<Host, Short> subscriptions;
	private final Map<UUID, Pair<Host, Short>> pendingAppendRequests;
	private final Map<Long, List<Pair<Host, Short>>> pendingReadRequest;

	private long nextEntryIndex;
	private HashMap<Long, String> logEntries;

	// Change this if required
	private final short underlyingProtocolID = 900;

	// Crypto info
	private Host myself;
	private PrivateKey privateKey;
	private PublicKey publicKey;

	private final HashMap<Host, PublicKey> publicKeys;

	public DistributedLogManager()
			throws IOException, ProtocolAlreadyExistsException, HandlerRegistrationException, GeneralSecurityException {

		super(DistributedLogManager.PROTO_NAME, DistributedLogManager.PROTO_ID);

		this.subscriptions = new HashMap<Host, Short>();
		this.pendingAppendRequests = new HashMap<UUID, Pair<Host, Short>>();
		this.pendingReadRequest = new HashMap<Long, List<Pair<Host, Short>>>();

		this.nextEntryIndex = 0;
		this.logEntries = new HashMap<Long, String>();

		this.publicKeys = new HashMap<Host, PublicKey>();
	}

	@Override
	public void init(Properties props) throws HandlerRegistrationException, IOException {
		
		if(!props.containsKey(SERVER_PORT_KEY)) {
			System.err.println("The configuration named '" + SERVER_PORT_KEY + "' is mandatory for the operation of the system.");
			System.exit(1);
		}
		
		
		Properties serverProps = new Properties();
		serverProps.put(SimpleServerChannel.ADDRESS_KEY, props.getProperty(ADDRESS_KEY));
		serverProps.setProperty(SimpleServerChannel.PORT_KEY, props.getProperty(SERVER_PORT_KEY));

		clientChannel = createChannel(SimpleServerChannel.NAME, serverProps);

		registerMessageSerializer(clientChannel, AppendToLog.MESSAGE_ID, AppendToLog.serializer);
		registerMessageSerializer(clientChannel, ReadLogEntryRequest.MESSAGE_ID, ReadLogEntryRequest.serializer);
		registerMessageSerializer(clientChannel, SubscribeToLog.MESSAGE_ID, SubscribeToLog.serializer);
		registerMessageSerializer(clientChannel, AppendToLogReply.MESSAGE_ID, AppendToLogReply.serializer);
		registerMessageSerializer(clientChannel, ReadLogEntryReply.MESSAGE_ID, ReadLogEntryReply.serializer);
		registerMessageSerializer(clientChannel, LogEntry.MESSAGE_ID, LogEntry.serializer);

		registerMessageHandler(clientChannel, AppendToLog.MESSAGE_ID, this::handleAppendToLogMessage);
		registerMessageHandler(clientChannel, ReadLogEntryRequest.MESSAGE_ID, this::handleReadLogEntryRequestMessage);
		registerMessageHandler(clientChannel, SubscribeToLog.MESSAGE_ID, this::handleSubscribeToLogMessage);

		registerChannelEventHandler(clientChannel, InConnectionDown.EVENT_ID, this::uponInConnectionDown);
		registerChannelEventHandler(clientChannel, InConnectionUp.EVENT_ID, this::uponInConnectionUp);
		registerChannelEventHandler(clientChannel, OutConnectionDown.EVENT_ID, this::uponOutConnectionDown);
		registerChannelEventHandler(clientChannel, OutConnectionUp.EVENT_ID, this::uponOutConnectionUp);
		registerChannelEventHandler(clientChannel, OutConnectionFailed.EVENT_ID, this::uponOutConnectionFailed);

		subscribeNotification(DeliverEntryNotification.NOTIFICATION_ID, this::handleDeliverEntryNotification);
		subscribeNotification(ChannelAvailable.NOTIFICATION_ID, this::handleChannelAvailableNotification);
		subscribeNotification(NeighborUp.NOTIFICATION_ID, this::handleNeighborUpNotification);
	}

	public void handleChannelAvailableNotification(ChannelAvailable not, short sourceProto) {
		this.myself = not.getMyHost();
		this.privateKey = not.getMyPrivateKey();
		this.publicKey = not.getMyPublicKey();

		this.publicKeys.put(myself, publicKey);
	}

	public void handleNeighborUpNotification(NeighborUp not, short protoSource) {
		this.publicKeys.put(not.getNeighbor(), not.getPublicKey());
	}

	public void handleAppendToLogMessage(AppendToLog msg, Host from, short sourceProto, int channelID) {
		try {
			AppendRequest req = new AppendRequest(msg.getClientID(), myself, msg.serializeRequest());
			req.sign(privateKey);

			pendingAppendRequests.put(req.getRequestID(), Pair.of(from, sourceProto));

			sendRequest(req, underlyingProtocolID);

		} catch (Exception e) {
			logger.error("Could not process client AppendToLogRequest", e);
		}
	}

	private void writeToLogAndFlush(long index, String logEntry) {
		this.logEntries.put(index, logEntry);
		logger.info("[[ " + index + " ]] " + logEntry );
	}
	
	public void handleDeliverEntryNotification(DeliverEntryNotification not, short protoID) {
		try {
			AppendToLog originalRequest = AppendToLog.deserializeRequest(not.getClientRequest());

			writeToLogAndFlush(nextEntryIndex, originalRequest.generateFullLogEntry());
			
			Pair<Host, Short> info = pendingAppendRequests.remove(not.getRequestID());
			if(info != null) {
				AppendToLogReply reply = new AppendToLogReply(originalRequest.getClientID(), nextEntryIndex);
				sendMessage(clientChannel, reply, info.getRight(), info.getLeft(), 0);
			} else {
				logger.warn("No pending append request found for delivered entry {}", not.getRequestID());
			}

			if (subscriptions.size() > 0) {

				LogEntry le = new LogEntry(nextEntryIndex, this.logEntries.get(nextEntryIndex));
				le.signMessage(privateKey);

				for (Entry<Host, Short> sub : subscriptions.entrySet()) {

				}

			}

			List<Pair<Host, Short>> pending = pendingReadRequest.remove(nextEntryIndex);

			if (pending != null) {

				ReadLogEntryReply readReply = new ReadLogEntryReply(nextEntryIndex,
						this.logEntries.get(nextEntryIndex));
				readReply.signMessage(privateKey);

				for (Pair<Host, Short> p : pending) {
					sendMessage(clientChannel, readReply, p.getRight(), p.getLeft(), 0);
				}
			}

			this.nextEntryIndex++;
		} catch (Exception e) {
			logger.error("Could not process a DeliverEntry notification", e);
		}
	}

	public void handleReadLogEntryRequestMessage(ReadLogEntryRequest msg, Host from, short sourceProto, int channelID) {
		try {
			if (this.logEntries.containsKey(msg.getEntryLogIndex())) {

				ReadLogEntryReply readReply = new ReadLogEntryReply(msg.getEntryLogIndex(),
						this.logEntries.get(msg.getEntryLogIndex()));
				readReply.signMessage(privateKey);

				sendMessage(clientChannel, readReply, sourceProto, from, 0);

			} else {
				// Store to reply at a latter date
				List<Pair<Host, Short>> pending = this.pendingReadRequest.get(msg.getEntryLogIndex());
				if (pending == null) {
					pending = new ArrayList<Pair<Host, Short>>();
					this.pendingReadRequest.put(msg.getEntryLogIndex(), pending);
				}

				pending.add(Pair.of(from, sourceProto));
			}
		} catch (Exception e) {
			logger.error("Could not process a DeliverEntry notification", e);
		}
	}

	public void handleSubscribeToLogMessage(SubscribeToLog msg, Host from, short sourceProto, int channelID) {
		this.subscriptions.put(from, sourceProto);
	}

	private void uponOutConnectionUp(OutConnectionUp event, int channel) {
		logger.info(event);
	}

	private void uponOutConnectionDown(OutConnectionDown event, int channel) {
		logger.warn(event);
	}

	private void uponOutConnectionFailed(OutConnectionFailed<ProtoMessage> ev, int ch) {
		logger.warn(ev);
	}

	private void uponInConnectionUp(InConnectionUp event, int channel) {
		logger.info(event);
	}

	private void uponInConnectionDown(InConnectionDown event, int channel) {
		logger.warn(event);
	}
}
