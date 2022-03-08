package hybrid.nodes.cloud_layer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import hybrid.FailureProducer;
import hybrid.nodes.Node;
import hybrid.nodes.core_network_layer.CNLNode;
import hybrid.nodes.edge_layer.EdgeNode;
import util_objects.Task;
import util_objects.Coordinates;
import util_objects.FailureHandler;
import util_objects.Irreplaceable;

/**
 * Represents the cloud layer. It extends {@link Node} and implements {@link FailureHandler} and {@link Irreplaceable}.<br>
 * Simultaneously it is a factory for {@link EdgeNode} and {@link CNLNode}
 * @author Marvin Kruber
 *
 */
public class Cloud extends Node implements FailureHandler, Irreplaceable{
	
	/** Singleton instance */
	private static Cloud singleton = null;
	
	/** Contains all {@CNLNode} of the system [stored by their corresponding ID]*/
	private Map<Long, CNLNode> cnlNodes;
	
	/** Contains all {@EdgeNode} of the system [stored by their corresponding ID]*/
	private Map<Long, EdgeNode> edgeNodes;
	
	/** Instance to regularly simulate node failures */
	private static FailureProducer failureProducer;
	
	private Cloud(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		Map<Long,CNLNode> cnlNodes = new HashMap<Long, CNLNode>();
		Map<Long,EdgeNode> edgeNodes = new HashMap<Long, EdgeNode>();
		this.cnlNodes = cnlNodes;
		this.edgeNodes = edgeNodes;
		System.out.println("[INFO] - CLOUD IS UP AND RUNNING");
	}

	/**
	 * Singleton method to get the cloud instance.
	 * @param coordinates geographical allocation of the cloud
	 * @param MAX_Storage maximum storage capacities of the cloud
	 * @param MAX_Ram maximum computation capacities of the cloud
	 * @return cloud instance
	 */
	public static Cloud getInstance(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM) {
		if(singleton == null) {
			singleton = (Cloud) (new Cloud(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM)).initiateGossip();
		}
		return singleton;
	}

	/**
	 * Factory methods for {@link EdgeNode} and {@link CNLNode}.
	 * @param layer - {@link Layer} of the new node
	 * @param IP_ADDRESS - IP address of the new node
	 * @param PORT - Port number of the new node
	 * @param NODE_ID - ID of the new node
	 * @param COORDINATES - {@link Coordinates} of the new node
	 * @param MAX_STORAGE - maximum storage capacity of the new node [in byte]
	 * @param MAX_RAM - maximum computation capacity of the new node [in byte]
	 * @return a new node
	 * @throws NoSuchElementException if the passed layer does not exist
	 */
	public Node generateNode(Layer layer, String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM)
		throws NoSuchElementException {
		Node newNode = null;
		switch(layer) {
			case Edge_Layer:
				CNLNode cnlNode = this.findClosestCNLNode(COORDINATES);
				newNode = (new EdgeNode(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, cnlNode)).initiateGossip();
				//Insert edge node
				cnlNode.insertEdgeNode((EdgeNode) newNode);
				this.edgeNodes.put(NODE_ID, (EdgeNode) newNode);
				this.stats.increaseNrOfEdgeNodes();
				break;
			case Core_Network_Layer:
				newNode = (new CNLNode(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, this)).initiateGossip();
				this.insertCNLNode((CNLNode) newNode);
				this.stats.increaseNrOfCNLNodes();
				break;
			default:
				throw new NoSuchElementException();
		}
		return newNode;
	}
	
	/** Determines the closest CNLNode based on the given coordinates.*/
	private CNLNode findClosestCNLNode(Coordinates coordinates) {
		return this.cnlNodes.values().stream()
					.sorted(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(coordinates)))
					.findFirst().get();
		//1. Determine the average range of a CNLNode (range of edge pools which are managed by it)
		//2. Find the correct edge pool (has to be done in CNL-Node)
	}
	
	/**
	 * Inserts a new {@link CNLNode} in the system and informs all other nodes of the cluster about the new node.
	 * @param node
	 */
	private void insertCNLNode(CNLNode node) {
		try {
			for (CNLNode clusterNode : this.cnlNodes.values()) {
				clusterNode.addClusterParticipant(node);
			}
		} catch (NoSuchElementException e) {
			System.err.println("[HINT] - There is no other CNLNode in the cluster");
		}
		this.cnlNodes.put(node.getNodeID(), node);
	}

	/**
	 * Removes a node from the system based on the passed NodeID.
	 * @param NodeID
	 * @throws NoSuchElementException if there is no suitable NodeID
	 */
	public void deleteNode(Layer layer, Node node) {
		switch(layer) {
			case Edge_Layer:
				this.findClosestCNLNode(node.getCoordinates()).deleteNode((EdgeNode) node);
				this.edgeNodes.remove(node.getNodeID());
				break;
			case Core_Network_Layer:
				this.cnlNodes.remove(node.getNodeID());
				this.cnlNodes.values().forEach(x -> x.removeNodeFromCluster(node.getNodeID()));
				node.shutdownNode();
				break;
			default:
				throw new NoSuchElementException();
		}
	}
	
	@Override
	public void shutdownNode() {
		failureProducer.interrupt();
		this.edgeNodes.values().forEach(x -> x.shutdownNode());
		this.cnlNodes.values().forEach(x -> x.shutdownNode());
		super.shutdownNode();
	}
	
	@Override
	public void redirectTask(Task task) {
		throw new IllegalArgumentException("[ERROR] - Capacity of the cloud would be exceeded");
	}
	
	//----------------------------------------------- Node Failure ------------------------------------------------------------
	
	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		System.out.println("[CLIENT-INFO] Node: " + failedNode.getNodeID() + "has to be checked. It could have failed");
		if(failedNode instanceof EdgeNode) {
			this.edgeNodes.remove(failedNode.getNodeID());
		} else if(failedNode instanceof CNLNode) {
			this.deleteNode(Layer.Core_Network_Layer, failedNode);
		} else {
			throw new RuntimeException("[ERROR] - UNKNOWN NODE");
		}
		this.stats.increaseNrOfDetectedNodeFailures();
	}	
	
	
	@Override
	public void fail() {
		throw new RuntimeException ("[ERROR] - The cloud does not fail!");
	}
	
	@Override
	public void startFailureRoutine(Node failedNode, Long NodeID) {
		synchronized(FailureHandler.reportedNodeFailures) {
			Set<Long> reporterIDs = FailureHandler.reportedNodeFailures.get(failedNode.getNodeID());
			//If at least two nodes reported an issue with failedNode it has to be removed
			if(reporterIDs == null ) { 
				reporterIDs = new HashSet<Long>();
				FailureHandler.reportedNodeFailures.put(failedNode.getNodeID(), reporterIDs);
			}
			reporterIDs.add(NodeID);
			if(reporterIDs.size() == 2) {
				this.reportNodeFailureToSupervisor(failedNode, this.getNodeID());
			}
		}
	}
	
	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		return this.findClosestCNLNode(clientPosition).checkForCloserNode(clientPosition);
	}
	
	/** Starts the distribution of node failures*/
	public void startDistributingNodeFailures() {
		failureProducer = FailureProducer.getInstance(edgeNodes, cnlNodes);
		failureProducer.start();
	}
	
	/**
	 * Enum for the construction of {@link EdgeNode} and {@link CNLNode}.
	 * @author Marvin Kruber
	 */
	public static enum Layer {
		Edge_Layer, Core_Network_Layer
	}
}
