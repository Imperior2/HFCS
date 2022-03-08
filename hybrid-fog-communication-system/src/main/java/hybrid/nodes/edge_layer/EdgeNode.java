package hybrid.nodes.edge_layer;

import java.util.Comparator;
import java.util.NoSuchElementException;

import hybrid.nodes.Node;
import hybrid.nodes.core_network_layer.CNLNode;
import util_objects.Coordinates;
import util_objects.NodeState;
import util_objects.Task;
/**
 * Represents a node of the edge layer and extends {@link Node}.
 * @author Marvin Kruber
 *
 */
public class EdgeNode extends Node{
	
	/** Supervisor of the edge node (-> {@link CNLNode})*/
	private CNLNode supervisor;
	
	/** Represents the radius of the corresponding pool*/
	private float poolRadius;
	
	public EdgeNode(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM, CNLNode supervisor) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		this.supervisor = supervisor;
		System.out.println("[INFO] - CREATED EDGE NODE WITH ID:" + NODE_ID);
	}

	@Override
	public void redirectTask(Task task) {
		try {
			Node target = this.clusterMetaData.values().stream()
				.filter(x -> ((long) x.getField("available_ram").getValue()) >= task.getRequiredRAM() &&
							((long) x.getField("available_storage").getValue()) >= task.getRequiredStorage())
				.min(Comparator.comparingDouble(x -> x.getAssociatedNode().getCoordinates().getDistance(this.getCoordinates()))).get().getAssociatedNode();
				
			//Check the capacities of the target node
			if(!target.checkRedirectedTaskRequest(task)) {
				throw new NoSuchElementException();
			}
		} catch (NoSuchElementException e){
			System.err.println("[ESCALATION] - ESCALATION TO THE CLOUD");
			this.sendTask(this.supervisor, task);
			this.stats.increaseNrOfEscalatedTask();
		}
	}
	

	/** Replaces the supervisor of the edge node. */
	public void replaceSupervisor(CNLNode newSupervisor) {
		this.supervisor = newSupervisor;
	}
	
	/**
	 * Registers a new cluster participant in cluster clusterMetaData
	 * @param nodeID - ID of the new node
	 * @param metaData - metadata information of the new node
	 */
	public void addClusterParticipant(Long nodeID, NodeState metaData) {
		synchronized(this.clusterMetaData) {
			this.clusterMetaData.put(nodeID, metaData);
		}
	}

	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		this.supervisor.startFailureRoutine(failedNode, NodeID);
	}
	
	/** Stores the ID of the edge pool to which the edge node belongs.
	 * @param poolID - ID of the edge pool to which the edge node is added*/ 
	public void setPoolID(int poolID) {
		this.pool_ID = poolID;
	}

	//----------------------------------------------- Client ------------------------------------------------------------
	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		double distanceToNode = this.getCoordinates().getDistance(clientPosition);
		if (distanceToNode <= (this.poolRadius / 2)) {
			return this;
		} else if (distanceToNode > (this.poolRadius / 2) && distanceToNode <= this.poolRadius) {
			//No Catch clause because at least this node is contained in the metadata
			return this.clusterMetaData.values().stream()
					.map(x -> x.getAssociatedNode())
					.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(clientPosition)))
					.get();
		}
		Node closerNode = this.supervisor.checkForCloserNode(clientPosition);
		return (closerNode == null) ? this : closerNode;
	}
	
	/** Setter for {@link EdgeNode#poolRadius} */
	public void setPoolDistance(float poolRadius) {
		this.poolRadius = poolRadius;
	}
	
}
