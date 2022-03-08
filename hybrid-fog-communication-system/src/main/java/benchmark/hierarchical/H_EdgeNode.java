package benchmark.hierarchical;

import hybrid.nodes.Node;
import util_objects.Coordinates;
import util_objects.Task;

/**
 * Represents an edge node in the hierarchical benchmark system. It extends {@link Node}. <br>
 * Thereby, the {@link Node#clusterMetaData} only contains the entry of this node. Furthermore the nodes only communicate via their
 * supervisor.
 * @author Marvin Kruber
 *
 */
public class H_EdgeNode extends Node {
	
	/** Supervisor {@link H_CNLNode} which handles the whole communication and administrative tasks*/
	private H_CNLNode supervisor;

	public H_EdgeNode(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM, H_CNLNode supervisor) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		this.supervisor = supervisor;
	}
	
	@Override
	public Node initiateGossip() {
		if(this.gossiper == null) {
			this.gossiper = new H_GossipThread(this, this.clusterMetaData, this.supervisor);
			this.gossiper.updateNodeState();
			this.gossiper.start();			
		}
		return this;
	}
	
	@Override
	public void redirectTask(Task task) {
		this.stats.increaseNrOfEscalatedTask();
		this.supervisor.receiveEscalatedTask(task);
	}

	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		return this.supervisor.checkForCloserNode(clientPosition);
	}

	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		this.supervisor.startFailureRoutine(failedNode, NodeID);
	}

}
