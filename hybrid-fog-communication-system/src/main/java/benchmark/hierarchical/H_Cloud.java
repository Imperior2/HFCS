package benchmark.hierarchical;
import java.util.Comparator;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import hybrid.nodes.Node;
import util_objects.Coordinates;
import util_objects.FailureHandler;
import util_objects.Irreplaceable;
import util_objects.Task;

/**
 * Represents the cloud in the hierarchical benchmark system. It extends {@link Node}. <br>
 * Thereby, the {@link Node#clusterMetaData} only contains the entry of this node. Furthermore the nodes only communicate via their
 * supervisor.
 * @author Marvin Kruber
 *
 */
public class H_Cloud extends Node implements FailureHandler, Irreplaceable {
	
	/** Singleton instance*/
	private static H_Cloud singleton = null;
	
	/** Set of {@link H_CNLNodes} which are supervised by the cloud*/
	private Set<H_CNLNode> supervisedCNLNodes = new HashSet<>();

	private H_Cloud(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
	}
	
	/**
	 * Singleton method to get the {@link H_Cloud} instance.
	 * @param coordinates geographical allocation of the cloud
	 * @param MAX_Storage maximum storage capacities of the cloud
	 * @param MAX_Ram maximum computation capacities of the cloud
	 * @return {@link H_Cloud} instance
	 */
	public static H_Cloud getInstance(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM) {
		if(singleton == null) {
			singleton = (H_Cloud) (new H_Cloud(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM)).initiateGossip();
		}
		return singleton;
	}

	@Override
	public Node initiateGossip() {
		if(this.gossiper == null) {
			this.gossiper = new H_GossipThread(this, this.clusterMetaData, null);
			this.gossiper.updateNodeState();		
		}
		return this;
	}

	@Override
	public void redirectTask(Task task) {
		throw new RuntimeException("[ERROR] - CAPACITIES OF THE CLOUD WOULD BE EXCEEDED");
	}
	
	/** Receives a task which exceeded the capacity of a {@link H_CNLNode}. If the capacities of the H_Cloud are not sufficient
	 *  a {@link RuntimeException} is thrown */
	public void receiveEscalatedTask(Task task) {
		if(!this.checkAndProcessTask(task)) {
			this.redirectTask(task);
		}
	}

	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		return this.clusterMetaData.values().stream().map(x -> x.getAssociatedNode())
				.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(clientPosition))).get();
	}

	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		System.err.println("[ERROR] - NODE " + failedNode.getNodeID() + " SEEMS TO HAVE FAILED.");
		
	}

	@Override
	public void startFailureRoutine(Node failedNode, Long NodeID) {
		synchronized(this.clusterMetaData) {
			failedNode.shutdownNode();
			this.clusterMetaData.remove(failedNode.getNodeID());
			this.supervisedCNLNodes.remove(failedNode);
		}
		this.stats.increaseNrOfDetectedNodeFailures();
	}
	
	/** 
	 * Adds a {@link H_CNLNode} to the set of nodes which are supervised by the cloud.
	 * @param edgeNode
	 */
	public void addCNLNode(H_CNLNode cnlNode) {
		synchronized(this.supervisedCNLNodes) {
			this.supervisedCNLNodes.add(cnlNode);
		}
	}
	
	/** 
	 * Removes a {@link H_CNLNode} from the set of nodes which are supervised by the cloud.
	 * @param edgeNode
	 */
	public void removeCNLNode(H_CNLNode cnlNode) {
		synchronized(this.supervisedCNLNodes) {
			this.supervisedCNLNodes.remove(cnlNode);
		}
	}
	
	@Override
	public void shutdownNode() {
		this.supervisedCNLNodes.forEach(x -> x.shutdownNode());
		super.shutdownNode();
	}
	
	/**
	 * Factory methods for {@link H_EdgeNode} and {@link H_CNLNode}.
	 * @param layer - {@link H_Layer} of the new node
	 * @param IP_ADDRESS - IP address of the new node
	 * @param PORT - Port number of the new node
	 * @param NODE_ID - ID of the new node
	 * @param COORDINATES - {@link Coordinates} of the new node
	 * @param MAX_STORAGE - maximum storage capacity of the new node [in byte]
	 * @param MAX_RAM - maximum computation capacity of the new node [in byte]
	 * @return a new node
	 * @throws NoSuchElementException if the passed layer does not exist
	 */
	public Node generateHNode(H_Layer layer, String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, 
			long MAX_STORAGE, long MAX_RAM) throws NoSuchElementException {
				Node newNode = null;
				switch(layer) {
					case Edge_Layer:
						H_CNLNode cnlNode = this.supervisedCNLNodes.stream()
							.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(COORDINATES))).get();
						newNode = (new H_EdgeNode(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, cnlNode)).initiateGossip();
						//Insert edge node
						cnlNode.addEdgeNode((H_EdgeNode)newNode);
						this.stats.increaseNrOfEdgeNodes();
						break;
					case Core_Network_Layer:
						newNode = (new H_CNLNode(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, this)).initiateGossip();
						this.addCNLNode((H_CNLNode) newNode);
						this.stats.increaseNrOfCNLNodes();
						break;
					default:
						throw new NoSuchElementException();
				}
				return newNode;
	}
	
	/**
	 * Enum for the construction of {@link H_EdgeNode}s and {@link H_CNLNode}s.
	 * @author Marvin Kruber
	 */
	public static enum H_Layer {
		Edge_Layer, Core_Network_Layer
	}
}
