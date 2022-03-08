package hybrid.nodes.edge_layer;

import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import hybrid.nodes.core_network_layer.CNLNode;
import util_objects.Coordinates;

/**This class represents an edge pool. An edge pool is a set of {@link EdgeNode}s which are 
 * collected based on their geographical proximity. An edge pool exhibits a hexagonal form. */
public class EdgePool {
	
	/** All edge nodes of the corresponding edge pool*/
	private Set<EdgeNode> edgeNodes;
	
	/** Maximum size of an edge pool */
	private final int MAXIMUM_POOL_SIZE = 30;
	
	/** All subpools of the current edge pool. <br>
	 * If an edge pool exceeds the limit of 30 edge nodes it is split into 7 new (sub) edge pools. 
	 * [one per edge of the current pool] */
	private Set<EdgePool> subpools = null;
	
	/** Marks the central point of the edge pool*/
	private final Coordinates CENTER;
	
	/** Represents the distance to the next edge pool [from center to center] */
	private final float DISTANCE_TO_OTHER_EDGE_POOL;
	
	/** ID generator for all edge pools */
	private static AtomicInteger poolIDCounter = new AtomicInteger(1);
	
	/** ID of the edge pool */
	private int poolID;
	
	/**
	 * Creates a new {@link EdgePool} based on the given coordinates of the pools center and its distance to other edge pools.
	 * @param center - {@link EdgePool#CENTER} of the edge pool
	 * @param distanceToOtherEdgePool - {@link EdgePool#DISTANCE_TO_OTHER_EDGE_POOL}
	 */
	public EdgePool(Coordinates center, float distanceToOtherEdgePool) {
		this.CENTER = center;
		this.DISTANCE_TO_OTHER_EDGE_POOL = distanceToOtherEdgePool;
		this.edgeNodes = new HashSet<EdgeNode>(MAXIMUM_POOL_SIZE);
		this.poolID = poolIDCounter.getAndIncrement();
	}
	
	/**
	 * Creates a new {@link EdgePool} based on the given coordinates of the pools center and its distance to other edge pools.
	 * @param center - {@link EdgePool#CENTER} of the edge pool
	 * @param edgeNode - first {@link EdgeNode} of the edge pool
	 * @param distanceToOtherEdgePool - {@link EdgePool#DISTANCE_TO_OTHER_EDGE_POOL}
	 */
	public EdgePool(EdgeNode edgeNode, Coordinates center, float distanceToOtherEdgePool) {
		this(center, distanceToOtherEdgePool);
		this.edgeNodes.add(edgeNode);
		edgeNode.setPoolID(this.poolID);
	}
	
	
	/**
	 * Adds an edge node to the pool.
	 * @param edgeNode - {@link EdgeNode} which should be added to the pool
	 */
	public void addNode(EdgeNode edgeNode) {
		synchronized(this.edgeNodes) {
			if(this.edgeNodes.size() == MAXIMUM_POOL_SIZE) {
				this.splitPool(edgeNode); //Invokes addNode on the subpools for each node
			} else {
				this.updateClusterMetadata(edgeNode);
				this.edgeNodes.add(edgeNode);
				edgeNode.setPoolID(this.poolID);
				edgeNode.setPoolDistance(DISTANCE_TO_OTHER_EDGE_POOL / 2);
			}
		}
	}
	
	/**
	 * Splits the current edge pool into seven new subpools and assigns the new edge node to the corresponding edge pool.
	 * @param edgeNode - {@link EdgeNode} which should be added to the pool
	 */
	private void splitPool(EdgeNode edgeNode) {
		this.subpools = new HashSet<EdgePool>(7);
		float newDistance = this.DISTANCE_TO_OTHER_EDGE_POOL / 4; //new distance from center to center
		this.subpools.add(new EdgePool(this.CENTER, newDistance));
		
		Coordinates newCenter;
		for(Direction direction : Direction.values()) { //Creates remaining sub edge pool according to their direction
			newCenter = this.calculateCenterOfSubPool(direction, newDistance);
			this.subpools.add(new EdgePool(newCenter, newDistance));
		}
		//-------------------------------------------------------------------------------------------------------------
		//Distributes the edge nodes among the newly created (sub) edge pools based on their geographical proximity
		Set<EdgePool> subpools = this.getAllSubpools();
		for(EdgeNode node : this.edgeNodes) {
			this.findClosestSubPool(node.getCoordinates(), subpools).addNode(node);
		}
		this.edgeNodes.clear();
		//-------------------------------------------------------------------------------------------------------------
		EdgePool pool = this.findClosestSubPool(edgeNode.getCoordinates());
		pool.updateClusterMetadata(edgeNode);
		pool.addNode(edgeNode); //Adds edge node number 31
	}
	
	/**
	 * Calculates the coordinates of an edge pool, which is vertical or diagonal to the current pool.
	 * @param direction - direction of the new center
	 * @param newDistance - new distance from center to center
	 * @return {@link Coordinates} of the center of the new sub edge pool
	 * @throws NoSuchElemenException if the passed {@link Direction} does not exist
	 */
	private Coordinates calculateCenterOfSubPool(Direction direction, float newDistance) {
		Coordinates newCenter = null;
		// Calculates new X- (which equals Y-Distance) based on the assumption of a regular hexagon i.e. an angle of 30°
		float oppositeSide = (float) (Math.sin(Math.toRadians(30)) * newDistance);
		float adjacent = (float) (Math.cos(Math.toRadians(30)) * newDistance);
		switch(direction) {
			case UP:
				newCenter = new Coordinates(this.CENTER.getX(), this.CENTER.getY() + newDistance);
				break;
			case UP_LEFT:
				newCenter = new Coordinates(this.CENTER.getX() - adjacent, this.CENTER.getY() + oppositeSide);
				break;
			case UP_RIGHT:
				newCenter = new Coordinates(this.CENTER.getX() + adjacent, this.CENTER.getY() + oppositeSide);
				break;
			case DOWN:
				newCenter = new Coordinates(this.CENTER.getX(), this.CENTER.getY() - newDistance);
				break;
			case DOWN_LEFT:
				newCenter = new Coordinates(this.CENTER.getX() - adjacent, this.CENTER.getY() - oppositeSide);
				break;
			case DOWN_RIGHT:
				newCenter = new Coordinates(this.CENTER.getX() + adjacent, this.CENTER.getY() - oppositeSide);
				break;
			default:
				throw new NoSuchElementException();
		}
		return newCenter;
	}
	
	/**
	 * Determines the closest (sub) edge pool based on the passed coordinates. 
	 * @param coordinates - location of the target
	 * @return closest {@link EdgePool}
	 */
	private EdgePool findClosestSubPool(Coordinates coordinates) {
		Set<EdgePool> targetPools = this.getAllSubpools();
		return this.findClosestSubPool(coordinates, targetPools);
	}
	
	/**
	 * Determines the closest (sub) edge pool out of the given edge pools based on the passed coordinates. 
	 * This is used as performance improvement in {@link EdgePool#distributeNodes()} .
	 * @param coordinates - location of the target
	 * @param targetPools - search set
	 * @return closest {@link EdgePool}
	 */
	private EdgePool findClosestSubPool(Coordinates coordinates, Set<EdgePool> searchSet) {
		return searchSet.stream().min(Comparator.comparingDouble(x -> x.getCenter().getDistance(coordinates))).get();
	}
	
	/**
	 * Used if a client changed his position so that the {@link CNLNode} has to assign the client a new edge node.
	 * @param coordinates - {@link Coordinates} of the client position
	 * @return closest EdgeNode of the pool {@EdgeNode}
	 */
	public EdgeNode findClosestEdgeNode(Coordinates coordinates) {
		EdgePool closestEdgePool = this.findClosestSubPool(coordinates);
		return closestEdgePool.edgeNodes.stream()
				.min(Comparator.comparingDouble(x -> x.getCoordinates().getDistance(coordinates))).get();
	}
	
	/**
	 * Removes a single node from the edge pool.
	 * @param node
	 */
	public void removeNode(EdgeNode node) {
		EdgePool targetPool = this.findClosestSubPool(node.getCoordinates());
		for(EdgeNode e : targetPool.edgeNodes) {
			e.removeNodeFromCluster(node.getNodeID());
		}
		node.shutdownNode();
	}
	
	/**
	 * Changes the supervisor of all edge nodes of the edge pool.
	 * @param newSupervisor
	 */
	public void changeSupervisor(CNLNode newSupervisor) {
		for(EdgePool pool : this.getAllSubpools()) {
			pool.edgeNodes.forEach(x -> x.replaceSupervisor(newSupervisor));
		}
	}
	
	/** Updates the metadata of an existing node in the cluster.*/
	private void updateClusterMetadata(EdgeNode node) {
		//Informs all other node of the cluster about the new node
		for (EdgeNode clusterNode : this.edgeNodes) {
			clusterNode.addClusterParticipant(node.getNodeID(), node.getNodeState());
		}
	}
	
	/**
	 * Shutdowns an edge pool by invoking {@link EdgeNode#shutdownNode()} on each pool member.
	 */
	public void shutDownPool() {
		synchronized(this.edgeNodes) {
			if(this.edgeNodes.size() >= 0 && this.subpools == null) {
				this.edgeNodes.forEach(x -> x.shutdownNode());
				
			} else if (this.edgeNodes.size() == 0 && this.subpools != null) {
				this.getAllSubpools().forEach(pool -> pool.shutDownPool());
			}
		}
	}
	
//================================  Getter   ===============================================
	/** @return number of edge nodes within the pool*/
	public int getPoolSize() {
		return this.edgeNodes.size();
	}
	
	/** @return geographical center of the pool*/
	public Coordinates getCenter() {
		return this.CENTER;
	}
	
	/** @return {@link EdgePool#DISTANCE_TO_OTHER_EDGE_POOL}*/
	public double getDistanceToOtherEdgePool() {
		return this.DISTANCE_TO_OTHER_EDGE_POOL;
	}
	
	/**@return all (direct and indirect) children egde pools */
	private Set<EdgePool> getAllSubpools() {
		Set<EdgePool> targetPools = new HashSet<EdgePool>();
		Queue<EdgePool> currentLayer = new LinkedList<EdgePool>();
		currentLayer.add(this);
		EdgePool pool;
		
		while(!currentLayer.isEmpty()) {
			pool = currentLayer.poll();
			if(pool.subpools != null) {
				currentLayer.addAll(pool.subpools);
			} else {
				targetPools.add(pool);	
			}
		}
		return targetPools;
	}
	
	//================================  STATS   ===============================================
	
	/** Returns a map which contains the ID of the current pool, all its subpools and the corresponding mebers of the pool<br>
	 *  This is just used for statistical things! 
	 *  @return set of IDs */
	public Map<Integer , Set<Long>> getAllPoolIDsAndMemberIDs(Map<Integer, Set<Long>> poolMembers) {
		if(this.subpools == null) {
			Set<Long> edgeNodeIDs = this.edgeNodes.stream().map(x -> x.getNodeID()).collect(Collectors.toSet());
			poolMembers.put(this.poolID, edgeNodeIDs);
		} else {
			this.subpools.forEach(x -> x.getAllPoolIDsAndMemberIDs(poolMembers));
		}
		return poolMembers;
	}

//================================  Util   ===============================================
	/**
	 * Direction of a new sub edge pool [viewed from the center of the current edge pool]
	 * @author Marvin Kruber
	 */
	private static enum Direction {
		UP, UP_RIGHT, UP_LEFT, DOWN, DOWN_RIGHT, DOWN_LEFT;
	}
}
