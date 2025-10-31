import java.io.IOException;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Properties;

import app.DistributedLogManager;
//import broadcast.CrashFaultReliableBroadcastProtocol;
import broadcast.byzantinereliablebcast.ByzantineReliableBroadcastProtocol;
import membership.staticmembership.StaticMembershipProtocol;
import replication.blockchain.BlockchainProtocol;
import pt.unl.fct.di.novasys.babel.core.Babel;
import pt.unl.fct.di.novasys.babel.exceptions.HandlerRegistrationException;
import pt.unl.fct.di.novasys.babel.exceptions.InvalidParameterException;
import pt.unl.fct.di.novasys.babel.exceptions.ProtocolAlreadyExistsException;
import replication.ReplicationProtocolPlaceholder;
import replication.pbft.PBFTReplicationProtocol;

public class Main {

	// Sets the log4j (logging library) configuration file and forces IPV4
	static {
		System.setProperty("log4j.configurationFile", "log4j2.xml");
		System.setProperty("java.net.preferIPv4Stack" , "true");
	}
	
	public static final String ADDRESS_KEY = "address";

	public static final void main(String[] args) throws IOException, InvalidParameterException, ProtocolAlreadyExistsException, HandlerRegistrationException, GeneralSecurityException {
		Properties props = Babel.loadConfig(Arrays.copyOfRange(args, 0, args.length), "babel-conf.txt");

		if (props.containsKey("interface")) {
			String address = Main.getAddress(props.getProperty("interface"));
			if (address == null)
				return;
			props.put(ADDRESS_KEY, address);
		}

		Babel babel = Babel.getInstance();

		DistributedLogManager dlm = new DistributedLogManager();
		//ReplicationProtocolPlaceholder rep = new ReplicationProtocolPlaceholder();
		BlockchainProtocol rep = new BlockchainProtocol();
		ByzantineReliableBroadcastProtocol bcast = new ByzantineReliableBroadcastProtocol();
		//PBFTReplicationProtocol pbft = new PBFTReplicationProtocol();
		
		//To simplify lets use this protocol again
		StaticMembershipProtocol smp = new StaticMembershipProtocol();
		
		babel.registerProtocol(dlm);
		babel.registerProtocol(rep);
		babel.registerProtocol(bcast);
		//babel.registerProtocol(pbft);
		
		babel.registerProtocol(smp);

		dlm.init(props);
		rep.init(props);
		bcast.init(props);
		//pbft.init(props);
		
		smp.init(props);

		babel.start();
		System.err.println("Babel has started...");

	}

    private static String getAddress(String inter) throws SocketException {
        NetworkInterface byName = NetworkInterface.getByName(inter);
        if (byName == null) {
        	System.err.println("No interface named " + inter);
            return null;
        }
        Enumeration<InetAddress> addresses = byName.getInetAddresses();
        InetAddress currentAddress;
        while (addresses.hasMoreElements()) {
            currentAddress = addresses.nextElement();
            if (currentAddress instanceof Inet4Address)
                return currentAddress.getHostAddress();
        }
        System.err.println("No ipv4 found for interface " + inter);
        return null;
    }
}
