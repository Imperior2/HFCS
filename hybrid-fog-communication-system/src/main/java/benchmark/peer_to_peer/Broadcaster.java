package benchmark.peer_to_peer;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import hybrid.Statistics;
import util_objects.NodeState;

/**
 * Central broadcast component which simulates the broadcast. It receives and distributes the metadata information updates 
 * of all {@link P2P_Node}s and distributes them. <br>
 * Thereby, a pub/sub-model is used.
 * @author Marvin Kruber
 *
 */
public class Broadcaster {
	
	/** Singleton instance */
	private static Broadcaster singleton = null;
	
	/** {@link Statistics}*/
	private Statistics stats = Statistics.getInstance();
	
	/** Contains all {@link P2P_Node}s of the cluster*/
	private Set<P2P_Node> clusterNodes = new HashSet<>();
	
	/** Creates a new Broadcaster*/
	private Broadcaster() {
	}
	
	/** Subscribe to the broadcast.
	 * @param subscriber */ 
	public void subscribe(P2P_Node subscriber) {
		synchronized(this.clusterNodes) {
			this.clusterNodes.add(subscriber);
		}
	}
	
	/** Unsubscribe the broadcast.
	 * @param unsubscriber */ 
	private void unsubscribe(P2P_Node unsubscriber) {
		synchronized(this.clusterNodes){
			this.clusterNodes.remove(unsubscriber);
		}
		
	}
	
	/**
	 * Publishes the {@link NodeState} of the sender to all other cluster participants.
	 * @param nodeID - of the sender
	 * @param update - new metadata information
	 */
	public void publishNodeStateUpdate(Long nodeID, NodeState update) {
		System.out.println("[INFO] - BROADCAST STARTED BY: " + nodeID);
		Set<P2P_Node> receivers; 
		synchronized(this.clusterNodes) {
			receivers = this.clusterNodes.stream().filter(x -> !x.getNodeID().equals(nodeID)).collect(Collectors.toSet());
			receivers.forEach(x -> System.out.print(x.getNodeID() + "; "));
		}
		for(P2P_Node receiver : receivers) {
			try {
				receiver.increaseMsgSent();
				receiver.receiveBroadcast(nodeID, update);
			} catch (NoSuchElementException e) {//Node failure handling
				System.err.println("[ERROR] - NODE FAILURE DETECTED. P2P-NODE: " + receiver.getNodeID());
				this.stats.increaseNrOfDetectedNodeFailures();
				this.unsubscribe(receiver);
				receiver.shutdownNode();
				for(P2P_Node n : this.clusterNodes) {
					n.removeNodeFromCluster(receiver.getNodeID());
				}
			}
		}
	}
	
	/**
	 * Singleton method
	 * @return singleton instance of Broadcaster
	 */
	public static Broadcaster getInstance() {
		if(singleton == null) {
			singleton = new Broadcaster();
		}
		return singleton;
	}
}
