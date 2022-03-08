package hybrid.nodes.core_network_layer;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

import hybrid.nodes.Node;
import hybrid.nodes.cloud_layer.Cloud;
import hybrid.nodes.edge_layer.EdgeNode;
import hybrid.nodes.edge_layer.EdgePool;
import util_objects.Task;
import util_objects.VersionedValue;
import util_objects.Coordinates;
import util_objects.FailureHandler;

/**
 * Represents a node of the core network layer. It extends {@link Node} and implements {@link FailureHandler}.
 * @author Marvin Kruber
 *
 */
public class CNLNode extends Node implements FailureHandler {
	
	/** Represents the geographical radius of an {@link EdgePool}*/
	public static final float EDGE_POOL_RANGE_CONFIG = 30; //TODO 10
	
	/** Supervisor of the CNLnode (-> {@link Cloud})*/
	private Cloud supervisor;
	
	/** All edge pools which are maintained by the CNLNode*/
	private Set<EdgePool> edgePools = new HashSet<EdgePool>();
	
	/** Version of the {@link CNLNode#edgePools} field*/
	private Long versionOfEdgePools = 0L;
	
	public CNLNode(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM, Cloud supervisor) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM);
		this.supervisor = supervisor;
		System.out.println("[INFO] - CREATED CNL NODE WITH ID:" + NODE_ID);
	}
	
	@Override
	public void shutdownNode() {
		super.shutdownNode();
		Map<Integer, Set<Long>> memberIDs = new HashMap<>();
		this.edgePools.forEach(pool -> {
			pool.shutDownPool();
			stats.storeIDsOfEdgePoolMembers(this.getNodeID(), pool.getAllPoolIDsAndMemberIDs(memberIDs));
		});
		stats.storeNumberOfEdgePools(this.getNodeID(), this.edgePools.size());
		
	}

	@Override
	public void redirectTask(Task task) {
		try {
			Node target = this.clusterMetaData.values().stream()
				.filter(x -> ((long) x.getField("available_ram").getValue()) >= task.getRequiredRAM() &&
							((long) x.getField("available_storage").getValue()) >= task.getRequiredStorage())
				.min(Comparator.comparingDouble(x -> x.getAssociatedNode().getCoordinates().getDistance(this.getCoordinates()))).get().getAssociatedNode();
			if(!target.checkRedirectedTaskRequest(task)) {
				throw new NoSuchElementException();
			}
		} catch (NoSuchElementException e){
			System.err.println("[ESCALATION] - ESCALATION TO THE CLOUD");
			this.sendTask(this.supervisor, task);
			this.stats.increaseNrOfEscalatedTask();
		}	
	}
	
	/**
	 * Inserts a new {@link EdgeNode} in the system. If there is no existing suitable {@link EdgePool}, a new one is created and
	 * the node is inserted.
	 * @param node new {@link EdgeNode}
	 */
	public void insertEdgeNode(EdgeNode node) {	
		synchronized(this.edgePools) {
			Coordinates targetPoolCoordinates = this.calculateCenterOfNewEdgePool(node.getCoordinates());
			try {
				EdgePool targetPool = this.edgePools.stream()
					.filter(x -> x.getCenter().equals(targetPoolCoordinates))
					.findAny().get();
				targetPool.addNode(node);
			} catch (NoSuchElementException e) {
				//The required edge pool has yet to be created
				EdgePool newPool = new EdgePool(node, targetPoolCoordinates, EDGE_POOL_RANGE_CONFIG); 
				this.edgePools.add(newPool);
				newPool.addNode(node);
				
				this.updateEdgePoolMetaData();
			}
		}
	}
	
	/** Adds newest information about the supervised edge pools to the to the clusterMetaData*/
	protected void updateEdgePoolMetaData() {
		synchronized(this.clusterMetaData) {
			this.clusterMetaData.get(this.getNodeID()).getFields()
				.put("edge_pools", new VersionedValue<>(new HashSet<>(this.edgePools), this.versionOfEdgePools));
			this.versionOfEdgePools++;
		}
	}
	
	/**
	 * Calculates the coordinates of a new {@link EdgePool} based on the coordinates of an {@link EdgeNode}, which should be 
	 * inserted into in this pool.
	 * Thereby, the intersection of prime meridian and equator is assumed as the origin of a two dimensional coordinates system. 
	 * @param coordinates of the {@link EdgeNode} which should be inserted in the new {@link EdgePool}
	 * @return coordinates of the center of the new {@link EdgePool}
	 */
	private Coordinates calculateCenterOfNewEdgePool(Coordinates coordinates) {
	//----------------------------------------------------------------------------------------------------------------
		/*Vertical movement parameters on the same horizontal line 
		==> Origin <_><_> 
		*/
			//horizontal distance between two hexagons on the same horizontal line
			float XMovementPossibility1 =  (float) (Math.cos(Math.toRadians(30)) * EDGE_POOL_RANGE_CONFIG * 2) ; 
			//vertical movement equals EDGE_POOL_RANGE_CONFIG in this movement scenario
			float YMovementPossibility1 = EDGE_POOL_RANGE_CONFIG;
	//----------------------------------------------------------------------------------------------------------------
		//Radius of the perimeter of the hexagon
		//Use geometric relations of a regular hexagon @see http://www.mathematische-basteleien.de/sechseck.htm
		float perimeterRadius = (float) (Math.cos(Math.toRadians(30)) / (EDGE_POOL_RANGE_CONFIG / 2));
		
		//Calculate position of potential suitable edge pool center
		float nrOfPassedHorizontalHexagons = coordinates.getX() / XMovementPossibility1;
		float potentialXCoordinate = (Math.round(nrOfPassedHorizontalHexagons) * XMovementPossibility1);
		float xDeviation = coordinates.getX() - potentialXCoordinate;
		
		float nrOfPassedVerticalHexagons = coordinates.getY() / YMovementPossibility1;
		float potentialYCoordinate = (Math.round(nrOfPassedVerticalHexagons) * YMovementPossibility1);
		float yDeviation = coordinates.getY() - potentialYCoordinate;
		
		//Coordinates of the center of the potential edge pool
		Coordinates potentialCoordinates = new Coordinates(potentialXCoordinate, potentialYCoordinate);
		
		//Calculate the distance between the potential center of the pool and the position of the node
		float distanceToCenter = (float) potentialCoordinates.getDistance(coordinates);
		double angle = Math.atan(Math.abs(yDeviation) / Math.abs(xDeviation)); //positive angle
		angle = Math.toDegrees(angle);
		
		while(angle > 30) {
			angle -= 30;
		}
		
		//Calculate maximum radius of the hexagon for the given angle.
		double criticalRadius = (perimeterRadius - EDGE_POOL_RANGE_CONFIG /2) / 30 * angle + EDGE_POOL_RANGE_CONFIG /2;
		
		if (criticalRadius > distanceToCenter) {
			return potentialCoordinates;
		} else {
			//Corner cases, i.e. the borders of the regular hexagon are exceeded
			//Use the fact that a regular hexagon can be formed from six equilateral triangles
			if(Math.abs(xDeviation) <= 0.5 * perimeterRadius && yDeviation >= EDGE_POOL_RANGE_CONFIG /2) { 
				//UP
				return new Coordinates(potentialXCoordinate, potentialYCoordinate + EDGE_POOL_RANGE_CONFIG);
			} else if(xDeviation > 0.5 * perimeterRadius && yDeviation >= 0) { 
				//UP_RIGHT
				return new Coordinates(potentialXCoordinate + 2 * perimeterRadius, potentialYCoordinate + EDGE_POOL_RANGE_CONFIG / 2);
			} else if (xDeviation < -0.5 * perimeterRadius && yDeviation >= 0) {
				//UP_LEFT
				return new Coordinates(potentialXCoordinate - 2 * perimeterRadius, potentialYCoordinate + EDGE_POOL_RANGE_CONFIG / 2);
			} else if (Math.abs(xDeviation) <= 0.5 * perimeterRadius && yDeviation <= -EDGE_POOL_RANGE_CONFIG /2) {
				//DOWN
				return new Coordinates(potentialXCoordinate, potentialYCoordinate - EDGE_POOL_RANGE_CONFIG);
			} else if (xDeviation > 0.5 * perimeterRadius && yDeviation <= 0) {
				//DOWN_RIGHT
				return new Coordinates(potentialXCoordinate + 2 * perimeterRadius, potentialYCoordinate - EDGE_POOL_RANGE_CONFIG / 2);
			} else {
				//DOWN_LEFT --> xDeviation < -0.5 * perimeterRadius && yDeviation <= 0
				return new Coordinates(potentialXCoordinate - 2 * perimeterRadius, potentialYCoordinate - EDGE_POOL_RANGE_CONFIG / 2);
			}
		}
	}
	
	/**
	 * Determines and reassign all {@link EdgePool}s which are closer to a newly created {@CNLNode}.
	 * @param newNode
	 */
	private void reassignEdgePoolsAfterInsertion(CNLNode newNode) {
		synchronized(this.edgePools) {
			Set<EdgePool> reassignablePools = this.edgePools.stream().filter(pool -> pool.getCenter().getDistance(newNode.getCoordinates())
														< pool.getCenter().getDistance(this.getCoordinates()))
										.collect(Collectors.toSet());
		if(reassignablePools.size() > 0) {
			newNode.receiveEdgePools(reassignablePools);
		}
		this.edgePools.removeAll(reassignablePools);
		}
	}
	
	/**
	 * Receive {@link EdgePool}s which are closer to the position of the current node.
	 * @param newPools 
	 */
	private void receiveEdgePools(Set<EdgePool> newPools) {
		synchronized(this.edgePools) {
			this.edgePools.addAll(newPools);
		}
		newPools.forEach(x -> x.changeSupervisor(this));
	}
	
	/**
	 * Registers a new cluster participant in cluster clusterMetaData
	 * @param nodeID - ID of the new node
	 * @param metaData - metadata information of the new node
	 */
	public void addClusterParticipant(CNLNode node) {
		synchronized(this.clusterMetaData) {
			this.clusterMetaData.put(node.getNodeID(), node.getNodeState());
		}
		this.reassignEdgePoolsAfterInsertion(node);
	}

	/**
	 * Deletes an {@link EdgeNode} which is supervised by this CNLNode.
	 * @param node
	 */
	public void deleteNode(EdgeNode node) {
		try {
		EdgePool targetPool = this.edgePools.stream()
				.min(Comparator.comparingDouble(x -> x.getCenter().getDistance(node.getCoordinates()))).get();
		targetPool.removeNode(node);
		} catch (NoSuchElementException e) {
			System.err.println(e);
		}
	}
	
	//----------------------------------------------- Node Failure ------------------------------------------------------------
	
	@Override
	public void reportNodeFailureToSupervisor(Node failedNode, Long NodeID) {
		this.supervisor.startFailureRoutine(failedNode, NodeID);
	}

	@Override
	public void startFailureRoutine(Node failedNode, Long NodeID) {
		
		System.err.println("[INFO] - STARTED FAILURE ROUTINE FOR:" + failedNode.getNodeID() + " BY: " + this.getNodeID());
		synchronized(reportedNodeFailures) {
			Set<Long> reporterIDs = reportedNodeFailures.get(failedNode.getNodeID());
			System.out.println(reportedNodeFailures);
			//If at least three nodes reported an issue with failedNode it has to be removed
			if(reporterIDs == null) { 
				reporterIDs = new HashSet<Long>();
				reportedNodeFailures.put(failedNode.getNodeID(), reporterIDs);
				System.err.println("[INFO] - NODE FAILURE DETECTED. 1st REPORT" + failedNode.getNodeID());
			}
			reporterIDs.add(NodeID);
			if(reporterIDs.size() == 2) {
				this.deleteNode((EdgeNode) failedNode);
				this.supervisor.reportNodeFailureToSupervisor(failedNode, NodeID);
				System.err.println("[INFO] - NODE FAILURE DETECTED. REMOVED NODE:" + failedNode.getNodeID());
			}
		}
	}
	
	/**
	 * It reassigns the edge pools which were supervised by the failed {@link CNLNode} to the other nodes of the cluster.
	 * @param failedNode
	 */
	public void reassignEdgePoolsAfterFailure (CNLNode failedNode) {
		@SuppressWarnings("unchecked")
		Set<EdgePool> pools = (Set<EdgePool>) this.clusterMetaData.get(failedNode.getNodeID()).getField("edge_pools").getValue();
		Set<Node> allCnlNodes = this.clusterMetaData.values().stream().map(x -> x.getAssociatedNode()).collect(Collectors.toSet());
		CNLNode receiver; 
		System.err.println("REASSIGN EDGE POOLS!");
		
		//Reassign each pool to the closest CNLNode
		for(EdgePool pool : pools) {
			receiver = (CNLNode) allCnlNodes.stream()
				.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(failedNode.getCoordinates()))).get();
			Set<EdgePool> wrapper = new HashSet<EdgePool>();
			wrapper.add(pool);
			receiver.receiveEdgePools(wrapper);
			//Update the edge pool list
			receiver.updateEdgePoolMetaData();
		}
	}

	//----------------------------------------------- Client ------------------------------------------------------------
	@Override
	public Node checkForCloserNode(Coordinates clientPosition) {
		CNLNode closerCNLNode = (CNLNode) this.clusterMetaData.values().stream()
			.map(x -> x.getAssociatedNode())
			.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(clientPosition)))
			.get();
		try {
			return closerCNLNode.edgePools.stream().min(Comparator.comparingDouble(x -> x.getCenter().getDistance(clientPosition)))
				.get().findClosestEdgeNode(clientPosition);
		} catch (NoSuchElementException e) {
			//Case that there are no edge nodes which are supervised by the closest CNL Node
			System.err.println("[INFO] - The closest CNLNodes has no edge pools.");
		}
		return null;
	}
	
	
}
