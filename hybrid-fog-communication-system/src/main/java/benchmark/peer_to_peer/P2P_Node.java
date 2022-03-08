package benchmark.peer_to_peer;

import java.util.Comparator;
import java.util.Map;
import java.util.NoSuchElementException;

import hybrid.nodes.Node;
import hybrid.nodes.edge_layer.EdgeNode;
import util_objects.Coordinates;
import util_objects.NodeState;
import util_objects.Task;

/**
 * Represents a node in the peer-to-peer benchmark system. It extends {@link Node}. <br>
 * Thereby, the {@link Node#clusterMetaData} consists all nodes of the system. Furthermore the nodes are using broadcast instead of
 * gossip and the FailureProducer is started externally
 * @author Marvin Kruber
 *
 */
public class P2P_Node extends Node {
	
	/** Radius of the geographical sector for which the node is responsible <br>
	 * 	It is equivalent to poolRadius of {@link EdgeNode}*/
	private final float SECTOR_DISTANCE;

	/**
	 * Creates a new {@link P2P_Node}.
	 * @param IP_ADDRESS
	 * @param PORT
	 * @param NODE_ID
	 * @param COORDINATES
	 * @param MAX_STORAGE
	 * @param MAX_RAM
	 * @param SECTOR_DISTANCE - {@link P2P_Node#SECTOR_DISTANCE}
	 */
	public P2P_Node(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE,
			long MAX_RAM, float SECTOR_DISTANCE, Map<Long, NodeState> clusterMetaData) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		this.SECTOR_DISTANCE = SECTOR_DISTANCE;
		this.clusterMetaData = clusterMetaData;
	}
	
	@Override
	public Node initiateGossip() {
		if(this.gossiper == null) {
			this.gossiper = new P2P_GossipThread(this, this.clusterMetaData);
			this.gossiper.updateNodeState();
			this.gossiper.start();			
		}
		return this;
	}

	@Override
	public void redirectTask(Task task) {
		boolean requestAccepted = false;
		Node target;
		while (!requestAccepted){
			try {
				 target = this.clusterMetaData.values().stream()
					.filter(x -> ((long) x.getField("available_ram").getValue()) >= task.getRequiredRAM() &&
								((long) x.getField("available_storage").getValue()) >= task.getRequiredStorage())
					.min(Comparator.comparingDouble(x -> x.getAssociatedNode().getCoordinates().getDistance(this.getCoordinates())))
					.get().getAssociatedNode();
					
				//Check the capacities of the target node
				requestAccepted = target.checkRedirectedTaskRequest(task);
				this.stats.increaseNrOfRedirectedTasks();
			} catch (NoSuchElementException e){
				System.err.println("[WARN] - CURRENTLY THERE IS NO NODE WHICH COULD HANDLE THE TASK");
				//If there is no suitable receiver than wait and check whether there are now enough resources to process the task
				//Else try to redirect the task again
				try {
					Thread.sleep(500);
					requestAccepted = this.checkAndProcessTask(task); 
				} catch (InterruptedException e1) {
					System.err.println("[WARN] - REDIRECTION INTERRUPTED");
					break;
				}
			}
		}
	}

	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		double distanceToNode = this.getCoordinates().getDistance(clientPosition);
		if (distanceToNode <= this.SECTOR_DISTANCE) {
			return this;
		} else {
			//No Catch clause because at least this node is contained in the metadata
			return this.clusterMetaData.values().stream()
					.map(x -> x.getAssociatedNode())
					.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(clientPosition)))
					.get();
		}
	}

	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		System.err.println("[ERROR] - FAILURE DETECTION IS MANAGED BY THE BROADCASTER COMPONENT");
	}

	/**
	 * Receives the {@link NodeState} from another peer and updates the own metadata.
	 * @param receiver - target node
	 * @param receivedMetadata - metadata information update of the sender
	 * @throws NoSuchElementException if the node has failed.
	 */
	public void receiveBroadcast(Long senderID, NodeState receivedMetadata) throws NoSuchElementException {
		//Node Failure
		if(this.hasFailed.get()) {
			throw new NoSuchElementException("Node: "+ this.getNodeID() + " has failed");
		}	
		synchronized(this.clusterMetaData) {
			this.clusterMetaData.put(senderID, receivedMetadata);
		}
			
		this.msg_received.incrementAndGet();
		//this.msg_replied.incrementAndGet();
	}
}
