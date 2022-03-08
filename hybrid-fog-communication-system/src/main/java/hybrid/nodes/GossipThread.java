package hybrid.nodes;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import util_objects.NodeState;
import util_objects.VersionedValue;

/**
 * Represents a thread of a node. It extends {@link Thread}.
 * GossipThread handles the gossip communication of a node.
 * @author Marvin Kruber
 *
 */
public class GossipThread extends Thread {
	
	/** Represents the associated server */
	protected Node server;
	
	/** Stores the metadata versions of all nodes in the cluster.*/
	protected Map<Long, NodeState> clusterMetaData;
	
	/** Time interval for the gossip communication [in milliseconds] */
	protected final int gossipInterval = 3000;
	
	/** Indicates the current version of the nodes state. Changes with every executed task. */
	private long stateVersion = 0;
	
	/** Random generator*/
	private Random generator = new Random();
	
	/** Maximum number of randomly picked nodes for gossip communication */
	private final int MAX_NR_OF_DRAWS = 3;
	
	/**
	 * Creates a new {@link GossipThread} of a server for the gossip communication.
	 * @param server - associated node
	 */
	public GossipThread(Node server, Map<Long, NodeState> clusterMetaData) {
		this.server = server;
		this.clusterMetaData = clusterMetaData;
	}
	
	@Override
	public void run() {
		System.out.println("[INFO] - GOSSIP STARTED BY NODE: " + this.server.getNodeID());
		try {
			while(!this.isInterrupted()) {
				this.chooseRandomGossipPartners().forEach(x -> this.sendGossipTo(x));
				Thread.sleep(this.gossipInterval);
			}
		} catch (InterruptedException e) {
			System.err.println("[WARN] - GOSSIP WAS INTERRUPTED! NODE: " + this.server.getNodeID());
			this.interrupt();
		}
	}
	
	/**
	 * Propagates metadata information to the receiver and receives potential metadata updates of the receiver.
	 * @param receiver
	 */
	private void sendGossipTo(Node receiver) {
		Map<Long, NodeState> metadata;
		//System.out.println("[INFO] - SEND GOSSIP TO " + receiver.getNodeID() + " SENDER: " + this.server.getNodeID());
		synchronized(this.clusterMetaData) {
			 metadata = new HashMap<Long, NodeState>(this.clusterMetaData);
			 //Pass values through return type
			 this.server.increaseMsgSent();
			 try {
				 Map<Long, NodeState> updates = receiver.receiveAndRespondGossipFrom(this.server, metadata);
				 this.clusterMetaData.putAll(updates);
			 } catch (NoSuchElementException e) {
				System.err.println(e.getMessage());
				this.server.reportNodeFailureToSupervisor(receiver, this.server.getNodeID());
			 }
		}
	}
	
	/**
	 * Determines random nodes of the cluster for the next gossip interaction.
	 * @return a set of randomly chosen nodes (maximum number of chosen nodes is given by {@link GossipThread#MAX_NR_OF_DRAWS}
	 */
	private Set<Node> chooseRandomGossipPartners() {
		List<Node> knownNodes;
		synchronized(this.clusterMetaData) {
			knownNodes = this.clusterMetaData.entrySet().stream().filter(x -> !x.getKey().equals(server.getNodeID()))
					.map(x -> x.getValue().getAssociatedNode()).collect(Collectors.toList());
			
		}
		Set<Node> chosenNodes = new HashSet<Node>();
		
		if(knownNodes.size() > this.MAX_NR_OF_DRAWS) {
			
			/* Chooses randomly a node out of knownNodes.
			 * The set ensures that no duplicates are contained and takes care that the number of randomly picked 
			 * nodes may vary */
			for (int i = 0; i < this.MAX_NR_OF_DRAWS; i++) {
				chosenNodes.add(knownNodes.get(this.generator.nextInt(knownNodes.size())));
			}
			
		} else {
			chosenNodes.addAll(knownNodes);
		}
		return chosenNodes;
	}
	
	/** Updates the node state. This should be used when a task is started or completed, and .*/
	public void updateNodeState() {
		synchronized(this.clusterMetaData) {
			NodeState nodeState = new NodeState(server);
			nodeState.updateMetaData("coordinates", new VersionedValue<>(server.getCoordinates(), this.stateVersion));
			nodeState.updateMetaData("available_ram", new VersionedValue<>(server.getavailableRAM(), this.stateVersion));
			nodeState.updateMetaData("available_storage", new VersionedValue<>(server.getavailableStorage(), this.stateVersion));
			this.incrementStateVersion();
			this.clusterMetaData.put(this.server.getNodeID(), nodeState);
		}
	}
	
	/** Increments the version number of the nodes state by one */
	private void incrementStateVersion() {
		this.stateVersion++;
	}
	
	
	
}
