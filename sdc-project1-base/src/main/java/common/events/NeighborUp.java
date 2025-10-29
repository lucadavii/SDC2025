package common.events;

import java.security.PublicKey;

import pt.unl.fct.di.novasys.babel.generic.ProtoNotification;
import pt.unl.fct.di.novasys.network.data.Host;

public class NeighborUp extends ProtoNotification {

	public static final short NOTIFICATION_ID = 101;
	
	private final Host neighbor;
	private final PublicKey publicKey;
	
	public NeighborUp(Host neighbor, PublicKey publicKey) {
		super(NOTIFICATION_ID);
		this.neighbor = neighbor;
		this.publicKey = publicKey;
	}

	public Host getNeighbor() {
		return this.neighbor;
	}
	
	public PublicKey getPublicKey() {
		return this.publicKey;
	}
}
