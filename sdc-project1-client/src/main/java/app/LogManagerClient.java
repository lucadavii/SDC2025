package app;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import app.measurements.Measurements;
import app.measurements.Status;
import app.messages.client.messages.AppendToLog;
import app.messages.client.messages.AppendToLogReply;
import app.messages.client.messages.LogEntry;
import app.messages.client.messages.ReadLogEntryReply;
import app.messages.client.messages.ReadLogEntryRequest;
import app.messages.client.messages.SubscribeToLog;
import app.timers.ExpiredOperation;
import io.netty.channel.EventLoopGroup;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.core.GenericProtocol;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;
import pt.unl.fct.di.novasys.babel.generic.signed.InvalidSerializerException;
import pt.unl.fct.di.novasys.babel.generic.signed.SignedProtoMessage;
import pt.unl.fct.di.novasys.channel.simpleclientserver.SimpleClientChannel;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerDownEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerFailedEvent;
import pt.unl.fct.di.novasys.channel.simpleclientserver.events.ServerUpEvent;
import pt.unl.fct.di.novasys.network.NetworkManager;
import pt.unl.fct.di.novasys.network.data.Host;

public class LogManagerClient {

	// Sets the log4j (logging library) configuration file and forces IPV4
	static {
		System.setProperty("log4j.configurationFile", "log4j2.xml");
		System.setProperty("java.net.preferIPv4Stack" , "true");
	}
	
	private static final Logger logger = LogManager.getLogger(LogManagerClient.class);

	public final static String INTERFACE = "interface";
	public final static String ADDRESS = "address";

	public final static String PROTO_NAME = "LogManagerClient";
	public final static short PROTO_ID = 600;

	public final static String INITIAL_PORT = "initial_port";
	public final static String NUMBER_OF_CLIENTS = "clients";
	private short initial_port;
	private short number_of_clients;

    public final static String APP_SERVER_PROTO = "server_proto";
    private short application_proto_number;
	
	public final static String OPERATION_TIMEOUT = "operation_timeout";

	public final static String KEY_STORE_FILE = "key_store";
	public final static String KEY_STORE_PASSWORD = "key_store_password";

	public final static String SERVER_LIST = "server_list";

	public final static String STATS_PERIOD = "report_period";
	private long report_period; // miliseconds;

	public final static float probability_to_read = (float) 0.05;
	
	private long operation_timeout;

	private KeyStore keystore;

	ClientInstance[] clients;

	private Host[] servers;

	private Babel b;

	private Measurements m;

	public static void main(String[] args) throws InvalidParameterException, IOException, HandlerRegistrationException,
			ProtocolAlreadyExistsException, GeneralSecurityException {
		Properties props = Babel.loadConfig(Arrays.copyOfRange(args, 0, args.length), "babel-conf.txt");
		logger.debug(props);

		if (props.containsKey(INTERFACE)) {
			String address = getAddress(props.getProperty(INTERFACE));
			if (address == null)
				return;
			props.put(ADDRESS, address);
		}

		LogManagerClient opm = new LogManagerClient(props);
		opm.stats();

	}

	private long wallclock = 0;

	private void stats() {

		while (true) {
			try {
				Thread.sleep(this.report_period);
			} catch (Exception e) {
			} // Every 20 seconds

			wallclock += this.report_period;
			System.out.println(wallclock + "ms :" + m.getSummary());
		}
	}

	public LogManagerClient(Properties props)
			throws IOException, ProtocolAlreadyExistsException, HandlerRegistrationException, GeneralSecurityException {

		this.initial_port = Short.parseShort(props.getProperty(INITIAL_PORT));
		this.number_of_clients = Short.parseShort(props.getProperty(NUMBER_OF_CLIENTS));

		this.application_proto_number = Short.parseShort(props.getProperty(APP_SERVER_PROTO));
		
		this.operation_timeout = Long.parseLong(props.getProperty(OPERATION_TIMEOUT));

		this.report_period = Long.parseLong(props.getProperty(STATS_PERIOD));

		this.m = new Measurements(new Properties());

		String keyStoreLocation = props.getProperty(KEY_STORE_FILE);
		char[] password = props.getProperty(KEY_STORE_PASSWORD).toCharArray();

		this.keystore = KeyStore.getInstance(KeyStore.getDefaultType());

		try (FileInputStream fis = new FileInputStream(keyStoreLocation)) {
			this.keystore.load(fis, password);
		}

		String servers = props.getProperty(SERVER_LIST);
		String[] token = servers.split(",");
		ArrayList<Host> hosts = new ArrayList<>();
		for (String s : token) {
			String[] e = s.split(":");
			hosts.add(new Host(InetAddress.getByName(e[0]), Short.parseShort(e[1])));
		}
		this.servers = hosts.toArray(new Host[hosts.size()]);

		EventLoopGroup nm = NetworkManager.createNewWorkerGroup();

		this.b = Babel.getInstance();

		this.clients = new ClientInstance[this.number_of_clients];
		for (short i = 1; i <= this.number_of_clients; i++) {
			this.initial_port += this.servers.length;
			this.clients[i - 1] = new ClientInstance(i, this.initial_port, password, nm, b, m);
		}

		for (short i = 0; i < this.number_of_clients; i++) {
			// System.err.println("Initializing client: " + this.clients[i].client_name);
			this.clients[i].init(props);
		}

		// System.err.println("Starting Babel.");
		this.b.start();

		for (short i = 0; i < this.number_of_clients; i++) {
			// System.err.println("Starting client: " + this.clients[i].client_name);
			this.clients[i].startClient();
		}
	}

	protected class ClientInstance extends GenericProtocol {

		private short client_id;
		private String client_name;
		@SuppressWarnings("unused")
		private PublicKey identity;
		private PrivateKey key;
		private int[] clientChannel;

		private Host myPrimaryServer;
		private int myPrimaryChannel;

		private Babel b;

		private Random r;

		private Measurements m;

		private EventLoopGroup nm;
		
		private UUID currentRequest;
		private long timeoutTimerID;
		private long lastEntryObserved;
		private long startExecution;
		
		
		private static final String APPEND_OP_ID = "Append";
		private static final String READ_OP_ID = "Read";

		public ClientInstance(short client_id, short port, char[] password, EventLoopGroup nm, Babel b, Measurements m)
				throws KeyStoreException, ProtocolAlreadyExistsException, UnrecoverableKeyException,
				NoSuchAlgorithmException {
			super(LogManagerClient.PROTO_NAME + client_id, (short) (LogManagerClient.PROTO_ID + client_id));
			this.client_id = client_id;
			this.client_name = "client" + this.client_id;
			this.identity = keystore.getCertificate(client_name).getPublicKey();
			this.key = (PrivateKey) keystore.getKey(this.client_name, password);
			this.nm = nm;

			this.b = b;
			this.b.registerProtocol(this);

			this.r = new Random(System.currentTimeMillis());

			this.m = m;
			
			this.timeoutTimerID = -1;
			this.lastEntryObserved = 0;
		}
		
		@Override
		public void init(Properties props) throws HandlerRegistrationException, IOException {
			clientChannel = new int[servers.length];

			for (int i = 0; i < servers.length; i++) {

				Properties clientProps2 = new Properties();
				clientProps2.put(SimpleClientChannel.WORKER_GROUP_KEY, nm);
				clientProps2.put(SimpleClientChannel.ADDRESS_KEY, servers[i].getAddress().getHostAddress());
				clientProps2.put(SimpleClientChannel.PORT_KEY, String.valueOf(servers[i].getPort()));
				clientChannel[i] = createChannel(SimpleClientChannel.NAME, clientProps2);

				// message serializers and handlers
				registerMessageSerializer(clientChannel[i], AppendToLog.MESSAGE_ID, AppendToLog.serializer);
				registerMessageSerializer(clientChannel[i], ReadLogEntryRequest.MESSAGE_ID, ReadLogEntryRequest.serializer);
				registerMessageSerializer(clientChannel[i], SubscribeToLog.MESSAGE_ID, SubscribeToLog.serializer);
				registerMessageSerializer(clientChannel[i], AppendToLogReply.MESSAGE_ID, AppendToLogReply.serializer);
				registerMessageSerializer(clientChannel[i], ReadLogEntryReply.MESSAGE_ID, ReadLogEntryReply.serializer);
				registerMessageSerializer(clientChannel[i], LogEntry.MESSAGE_ID, LogEntry.serializer);
					
				registerMessageHandler(clientChannel[i], AppendToLogReply.MESSAGE_ID, this::handleAppentToLogReply);
				registerMessageHandler(clientChannel[i], ReadLogEntryReply.MESSAGE_ID, this::handleReadLogEntryReply);
				registerMessageHandler(clientChannel[i], LogEntry.MESSAGE_ID, this::handleLogEntry);
				
				registerChannelEventHandler(clientChannel[i], ServerDownEvent.EVENT_ID, this::uponServerDown);
				registerChannelEventHandler(clientChannel[i], ServerUpEvent.EVENT_ID, this::uponServerUp);
				registerChannelEventHandler(clientChannel[i], ServerFailedEvent.EVENT_ID, this::uponServerFailed);

				//System.err.println("Client " + client_name + " opening connection to " + servers[i] + " on channel " + clientChannel[i]);
				openConnection(servers[i], clientChannel[i]);
			}

			
			registerTimerHandler(ExpiredOperation.TIMER_ID, this::handleExpiredOperationTimer);

			this.myPrimaryChannel = clientChannel[(client_id -1) % servers.length];
			this.myPrimaryServer = servers[(client_id -1) % servers.length];
		}

		public void startClient() {	
			SubscribeToLog request = new SubscribeToLog(client_name, 0);
			try {
				request.signMessage(key);
			} catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException
					| InvalidSerializerException e) {
				System.err.println(client_name + " (" + client_id + "): Unable to sign client request.");
				e.printStackTrace();
				return;
			}
			
			sendMessage(myPrimaryChannel, request, application_proto_number, myPrimaryServer, 0);
			
			executeOperation();
		}
		
		public void executeOperation() {
			//System.err.println(this.client_name + ": Generating new operation...");

			SignedProtoMessage request = null;
			
			if(r.nextFloat() <= LogManagerClient.probability_to_read) {
				request = new ReadLogEntryRequest(client_name, this.lastEntryObserved >= 1 ? r.nextInt((int) this.lastEntryObserved) : 1);
				currentRequest = ((ReadLogEntryRequest)request).getRequestID();
			} else {
				 request = new AppendToLog(client_name, "Generated values: " + r.nextLong() + " :: " + r.nextLong() + " :: " + r.nextLong());
				 currentRequest = ((AppendToLog)request).getRequestID();
			}
			
			try {
				request.signMessage(key);
			} catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException
					| InvalidSerializerException e) {
				System.err.println(client_name + " (" + client_id + "): Unable to sign client request.");
				e.printStackTrace();
				return;
			}
			
			//System.err.println("Sending request of type " + request.getClass().getCanonicalName() + " to " + myPrimaryServer + " on " + myPrimaryChannel);
			sendMessage(myPrimaryChannel, request, application_proto_number, myPrimaryServer, 0);
			this.startExecution = System.currentTimeMillis();
			
			this.timeoutTimerID = setupTimer(new ExpiredOperation(currentRequest, request), operation_timeout);
		}
		
		private void clearTimeout() {
			if(timeoutTimerID != -1) {
				cancelTimer(timeoutTimerID);
				this.timeoutTimerID = -1;
			}
		}
		
		public void handleAppentToLogReply(AppendToLogReply msg, Host h, short protoId, int channel) {
			clearTimeout();
			m.measure(APPEND_OP_ID, (int) (System.currentTimeMillis() - this.startExecution));
			m.reportStatus(APPEND_OP_ID, Status.SUCCESS);
			
			//System.err.println("Received reply to append operation with index: " + msg.getEntryLogIndex());
			this.lastEntryObserved = Math.max(this.lastEntryObserved, msg.getEntryLogIndex());
			executeOperation();
		}
		
		public void handleReadLogEntryReply(ReadLogEntryReply msg, Host h, short protoId, int channel) {
			clearTimeout();
			m.measure(READ_OP_ID, (int) (System.currentTimeMillis() - this.startExecution));
			m.reportStatus(READ_OP_ID, Status.SUCCESS);
			
			//System.err.println("Received reply to read operation for index " + msg.getEntryLogIndex() + ": " + msg.getLogEntry());
			executeOperation();
		}
		
		public void handleLogEntry(LogEntry msg, Host h, short protoId, int channel) {
			clearTimeout();
			
			System.err.println("Received a new log entry with index " + msg.getIndex() + " and contents: " + msg.getLogEntry());
			
			this.lastEntryObserved = Math.max(this.lastEntryObserved, msg.getIndex());
		}
		
		public void handleExpiredOperationTimer(ExpiredOperation eo, long delay) {
			if(!eo.req.equals(currentRequest))
				return;

			if(eo.message instanceof AppendToLog) 
				m.reportStatus(APPEND_OP_ID, Status.FAILURE);
			else if(eo.message instanceof ReadLogEntryRequest)
				m.reportStatus(READ_OP_ID, Status.FAILURE);
			
			this.timeoutTimerID = -1;
			executeOperation();
		}
		
		private void uponServerDown(ServerDownEvent event, int channel) {
			logger.warn(client_name + " " + event);
			for(int i = 0; i < servers.length; i++) {
				if(servers[i].equals(event.getServer())) {
					servers[i] = null;
					break;
				}
			}
			
			if(this.myPrimaryServer.equals(event.getServer())) {
				while(true) {
					int pick = r.nextInt(servers.length);
					if(servers[pick] != null) {
						myPrimaryServer = servers[pick];
						myPrimaryChannel = clientChannel[pick];
						break;
					}
				}
			}
		}

		private void uponServerUp(ServerUpEvent event, int channel) {
			logger.debug(client_name + " " + event);
		}

		private void uponServerFailed(ServerFailedEvent event, int channel) {
			logger.warn(client_name + " " + event);
			for(int i = 0; i < servers.length; i++) {
				if(servers[i].equals(event.getServer())) {
					servers[i] = null;
					break;
				}
			}
			
			if(this.myPrimaryServer.equals(event.getServer())) {
				while(true) {
					int pick = r.nextInt(servers.length);
					if(servers[pick] != null) {
						myPrimaryServer = servers[pick];
						myPrimaryChannel = clientChannel[pick];
						break;
					}
				}
			}
		}
	}

	private static String getAddress(String inter) throws SocketException {
		NetworkInterface byName = NetworkInterface.getByName(inter);
		if (byName == null) {
			logger.error("No interface named " + inter);
			return null;
		}
		Enumeration<InetAddress> addresses = byName.getInetAddresses();
		InetAddress currentAddress;
		while (addresses.hasMoreElements()) {
			currentAddress = addresses.nextElement();
			if (currentAddress instanceof Inet4Address)
				return currentAddress.getHostAddress();
		}
		logger.error("No ipv4 found for interface " + inter);
		return null;
	}

}
