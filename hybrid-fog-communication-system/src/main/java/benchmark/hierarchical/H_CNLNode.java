package benchmark.hierarchical;

import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import hybrid.nodes.Node;
import util_objects.Coordinates;
import util_objects.FailureHandler;
import util_objects.NodeState;
import util_objects.Task;

/**
 * Represents a core network layer node in the hierarchical benchmark system. It extends {@link Node}. <br>
 * Thereby, the {@link Node#clusterMetaData} only contains the entry of this node. Furthermore the nodes only communicate via their
 * supervisor.
 * @author Marvin Kruber
 *
 */
public class H_CNLNode extends Node implements FailureHandler {

	/** Supervisor {@link H_Cloud} which handles the whole communication and administrative tasks*/
	private H_Cloud supervisor;
	
	private Set<H_EdgeNode> supervisedEdgeNodes = new HashSet<>();
	
	public H_CNLNode(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE,
			long MAX_RAM, H_Cloud supervisor) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		this.supervisor = supervisor;
	}
	
	@Override
	public Node initiateGossip() {
		if(this.gossiper == null) {
			this.gossiper = new H_GossipThread(this, this.clusterMetaData, supervisor);
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
	
	/** Receives a task which exceeded the capacity of a {@link H_EdgeNode}. If the capacities of this H_CNLNode are not sufficient
	 * the task is redirected to the cloud. */
	public void receiveEscalatedTask(Task task) {
		if(!this.checkAndProcessTask(task)) {
			this.redirectTask(task);
		}
	}

	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		//The cloud receives the metadata of all nodes so that it is able to determine the closest node
		return this.supervisor.checkForCloserNode(clientPosition);
	}

	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		this.supervisor.startFailureRoutine(failedNode, NodeID);
		
	}

	@Override
	public void startFailureRoutine(Node failedNode, Long NodeID) {
		synchronized(this.clusterMetaData) {
			this.clusterMetaData.remove(failedNode.getNodeID());
			this.supervisedEdgeNodes.remove(failedNode);
		}
		this.stats.increaseNrOfDetectedNodeFailures();
	}
	
	@Override
	public Map<Long, NodeState> receiveAndRespondGossipFrom(Node sender, Map<Long, NodeState> receivedMetadata) throws NoSuchElementException{
		//Node Failure
		if(this.hasFailed.get()) {
			System.out.println("Throw Exception");
			throw new NoSuchElementException("Node: "+ this.getNodeID());
		}	
		//Computes differences and add more current metadata information to the own cluster view.
		this.clusterMetaData.putAll(receivedMetadata);	
			
		this.msg_received.incrementAndGet();
		//this.msg_replied.incrementAndGet();
		return null;	
	}
	
	@Override
	public void shutdownNode() {
		this.supervisedEdgeNodes.forEach(x -> x.shutdownNode());
		super.shutdownNode();
	}
	
	/** 
	 * Adds a {@link H_EdgeNode} to the set of nodes which are supervised by this node.
	 * @param edgeNode
	 */
	public void addEdgeNode(H_EdgeNode edgeNode) {
		synchronized(this.supervisedEdgeNodes) {
			this.supervisedEdgeNodes.add(edgeNode);
		}
	}
	
	/** 
	 * Removes a {@link H_EdgeNode} from the set of nodes which are supervised by this node.
	 * @param edgeNode
	 */
	public void removeEdgeNode(H_EdgeNode edgeNode) {
		synchronized(this.supervisedEdgeNodes) {
			this.supervisedEdgeNodes.remove(edgeNode);
		}
	}

}
