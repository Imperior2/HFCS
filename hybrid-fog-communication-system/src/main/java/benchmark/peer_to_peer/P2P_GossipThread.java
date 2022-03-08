package benchmark.peer_to_peer;

import java.util.Map;

import hybrid.nodes.GossipThread;
import util_objects.NodeState;

/** Peer-to-peer version of {@link GossipThread}*/
public class P2P_GossipThread extends GossipThread{
	
	/** Associated P2P_Node -> Overrides server of {@link GossipThread}*/
	private P2P_Node server;
	
	/** Component for broadcasting {@link Broadcaster} */
	private final Broadcaster broadcaster = Broadcaster.getInstance();

	public P2P_GossipThread(P2P_Node server, Map<Long, NodeState> clusterMetaData) {
		super(server, clusterMetaData);
		this.server = server;
	}
	
	@Override
	public void run() {
		this.broadcaster.subscribe(this.server);
		try {
			while(!this.isInterrupted()) {
				//Broadcast
				this.broadcast();
				Thread.sleep(this.gossipInterval);
			}
		} catch (InterruptedException e) {
			System.err.println("[WARN] - P2P_GOSSIP WAS INTERRUPTED! NODE: " + this.server.getNodeID());
			this.interrupt();
		}
	}
	
	/** Sends the current {@link NodeState} of {@link P2P_GossipThread#server} to all other known nodes via { {@link Broadcaster}*/
	private void broadcast() {
		Long nodeId = this.server.getNodeID();
		NodeState state = this.server.getNodeState(); //Put in synchronized
		this.broadcaster.publishNodeStateUpdate(nodeId, state);
	}
}
