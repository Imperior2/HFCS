
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import benchmark.hierarchical.H_CNLNode;
import benchmark.hierarchical.H_Cloud;
import benchmark.hierarchical.H_Cloud.H_Layer;
import benchmark.hierarchical.H_EdgeNode;
import benchmark.peer_to_peer.P2P_Cloud;
import benchmark.peer_to_peer.P2P_Node;
import hybrid.Client;
import hybrid.FailureProducer;
import hybrid.Statistics;
import hybrid.nodes.cloud_layer.Cloud;
import hybrid.nodes.cloud_layer.Cloud.Layer;
import util_objects.Coordinates;
import util_objects.NodeState;

/**
 * This class starts the prototype and the benchmarks.
 * @author Marvin Kruber
 *
 */
public class Starter {
	
	private static Statistics stats = Statistics.getInstance();

	private static Random generator = new Random();
	
	/** Configures the maximum geographical longitude*/
	private static final int MAX_LONGITUDE_VALUE = 180;
	
	/** Configures the maximum geographical latitude*/
	private static final int MAX_LATITUDE_VALUE = 90;
	
	/** Configures the number of {@link CNLNode}s*/
	private static final int NR_OF_CNL_NODES = 2;//10;
	
	/** Configures the number of {@link EdgeNode}s*/
	private static final int NR_OF_EDGE_NODES = 10;//1_000;
	
	/** Configures the number of {@link Client}s*/
	private static final int NR_OF_CLIENTS = 50;
	
	/** Configures the maximum capacity of an edge node*/
	private static final int MAX_EDGE_NODE_CAPACITY = 50_000;
	
	/** Configures the minimum capacity of an edge node*/
	private static final int MIN_EDGE_NODE_CAPACITY = 10_000;
	
	/** Configures the time until the interrupt command is executed [in milliseconds]*/
	private static final int TIME_TILL_INTERRUPT = 60_000;//600_000;
	
	/** Configures the maximum capacity of a CNL node*/
	private static final int MAX_CNL_NODE_CAPACITY = 500_000_000;
	
	/** Configures the minimum capacity of a CNL node*/
	private static final int MIN_CNL_NODE_CAPACITY = 1_000_000;

//------------------------------------------------------ P2P Benchmark -------------------------------------------------------------------
	/** Configures the radius of the geographical sector for which a node is responsible*/
	private static final int P2P_DISTANCE = 5;

//------------------------------------------------------ Hierarchical Benchmark ----------------------------------------------------------
	
	public static void main(String[] args) {
		try {
			//simulateHybridApproach();
			//simulateP2PApproach();
			simulateHierarchicalApproach();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Generates random coordinates which are limited by MAX_LONGITUDE_VALUE and MAX_LATITUDE_VALUE
	 * @return new random coordinates
	 */
	private static Coordinates generateRandomCoordinates() {
		int xDirection = (generator.nextBoolean()) ? -1 : 1; 
		int yDirection = (generator.nextBoolean()) ? -1 : 1;
		float xCoordinate = xDirection * generator.nextFloat() * generator.nextInt(MAX_LONGITUDE_VALUE);
		float yCoordinate = yDirection * generator.nextFloat() * generator.nextInt(MAX_LATITUDE_VALUE);
		return new Coordinates(xCoordinate, yCoordinate);
	}
	
	/**
	 * Initializes the HFCS-prototype and starts the simulation based on the configuration of {@link Starter}.
	 * @throws InterruptedException if the simulation is interrupted
	 */
	private static void simulateHybridApproach() throws InterruptedException {
		long ID = 1L;
		
		Cloud cloud = Cloud.getInstance("IP", 2000, ID, generateRandomCoordinates(), Integer.MAX_VALUE, Integer.MAX_VALUE);
		cloud.initiateGossip();
		ID++;
		
		for(int i = 0; i < NR_OF_CNL_NODES; i++) {
			cloud.generateNode(Layer.Core_Network_Layer, "IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY);
			ID++;
			Thread.sleep(500);
		}
		
		for(int i = 0; i < NR_OF_EDGE_NODES; i++) {
			cloud.generateNode(Layer.Edge_Layer, "IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_EDGE_NODE_CAPACITY) + MIN_EDGE_NODE_CAPACITY, 
					generator.nextInt(MAX_EDGE_NODE_CAPACITY) + MIN_EDGE_NODE_CAPACITY);
			ID++;
			Thread.sleep(200);
		}
		
		cloud.startDistributingNodeFailures();
		
		Set<Client> clients = new HashSet<>(20);
		for(int i = 0; i < NR_OF_CLIENTS; i++) {
			Client client = new Client(generateRandomCoordinates(), cloud);
			client.start();
			clients.add(client);
			Thread.sleep(200);
		}
		
		try {
			Thread.sleep(TIME_TILL_INTERRUPT);
			clients.forEach(x -> x.interrupt());
			cloud.shutdownNode();
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		stats.writeStatisticsToCSVFile("./src/main/resources/simple_data.csv", "./src/main/resources/complex_data.csv", "./src/main/resources/edge_pool_stats.csv");
	}
	
	/**
	 * Initializes the peer-to-peer benchmark and starts the simulation based on the configuration of {@link Starter}.
	 * @throws InterruptedException if the simulation is interrupted
	 */
	private static void simulateP2PApproach() throws InterruptedException {
		long ID = 1L;
		
		//CNL-Nodes and edge nodes are only distinguished because of different failure likelihoods
		Map<Long, P2P_Node> cnlNodes = new HashMap<>(NR_OF_CNL_NODES); 
		Map<Long, P2P_Node> edgeNodes = new HashMap<>(NR_OF_EDGE_NODES);
		Map<Long, NodeState> clusterMetaData = new HashMap<>();
		
		P2P_Cloud cloud = P2P_Cloud.getInstance("IP", 2000, ID, generateRandomCoordinates(), Integer.MAX_VALUE, Integer.MAX_VALUE, P2P_DISTANCE);
		cloud.initiateGossip();
		ID++;
		
		P2P_Node node;
		
		for(int i = 0; i < NR_OF_CNL_NODES; i++) {
			node = new P2P_Node("IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, P2P_DISTANCE, clusterMetaData);
			node.initiateGossip();
			cnlNodes.put(ID, node);
			ID++;
			stats.increaseNrOfCNLNodes();
			Thread.sleep(200);
		}
		
		for(int i = 0; i < NR_OF_EDGE_NODES; i++) {
			node = new P2P_Node("IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, P2P_DISTANCE, clusterMetaData);
			node.initiateGossip();
			edgeNodes.put(ID, node);
			ID++;
			stats.increaseNrOfEdgeNodes();
			Thread.sleep(200);
		}
		
		FailureProducer fp = FailureProducer.getInstance(edgeNodes, cnlNodes);
		fp.start();
		
		Set<Client> clients = new HashSet<>(20);
		for(int i = 0; i < NR_OF_CLIENTS; i++) {
			Client client = new Client(generateRandomCoordinates(), cloud);
			client.start();
			clients.add(client);
			Thread.sleep(200);
		}
		
		try {
			Thread.sleep(TIME_TILL_INTERRUPT);
			fp.interrupt();
			clients.forEach(x -> x.interrupt());
			cloud.shutdownNode();
			cnlNodes.values().forEach(x -> x.shutdownNode());
			edgeNodes.values().forEach(x -> x.shutdownNode());
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		stats.writeStatisticsToCSVFile("./src/main/resources/simple_data_p2p.csv", "./src/main/resources/complex_data_p2p.csv", null);
	}
	
	/**
	 * Initializes the hierarchical benchmark and starts the simulation based on the configuration of {@link Starter}.
	 * @throws InterruptedException if the simulation is interrupted
	 */
	private static void simulateHierarchicalApproach() throws InterruptedException {
		long ID = 1L;
		
		Map<Long, H_CNLNode> cnlNodes = new HashMap<>(NR_OF_CNL_NODES); 
		Map<Long, H_EdgeNode> edgeNodes = new HashMap<>(NR_OF_EDGE_NODES);
		
		H_Cloud cloud = H_Cloud.getInstance("IP", 2000, ID, generateRandomCoordinates(), Integer.MAX_VALUE, Integer.MAX_VALUE);
		cloud.initiateGossip();
		ID++;
		
		H_CNLNode cnlNode;
		H_EdgeNode edgeNode;
		
		for(int i = 0; i < NR_OF_CNL_NODES; i++) {
			cnlNode = (H_CNLNode) cloud.generateHNode(H_Layer.Core_Network_Layer, "IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY, 
					generator.nextInt(MAX_CNL_NODE_CAPACITY) + MIN_CNL_NODE_CAPACITY);
			cnlNodes.put(ID, cnlNode);
			ID++;
			Thread.sleep(500);
		}
		
		for(int i = 0; i < NR_OF_EDGE_NODES; i++) {
			edgeNode = (H_EdgeNode) cloud.generateHNode(H_Layer.Edge_Layer, "IP", 480, ID, generateRandomCoordinates(), 
					generator.nextInt(MAX_EDGE_NODE_CAPACITY) + MIN_EDGE_NODE_CAPACITY, 
					generator.nextInt(MAX_EDGE_NODE_CAPACITY) + MIN_EDGE_NODE_CAPACITY);
			edgeNodes.put(ID, edgeNode);
			ID++;
			Thread.sleep(200);
		}
		FailureProducer fp = FailureProducer.getInstance(edgeNodes, cnlNodes);
		fp.start();
		
		Set<Client> clients = new HashSet<>(20);
		for(int i = 0; i < NR_OF_CLIENTS; i++) {
			Client client = new Client(generateRandomCoordinates(), cloud);
			client.start();
			clients.add(client);
			Thread.sleep(200);
		}
		
		try {
			Thread.sleep(TIME_TILL_INTERRUPT);
			fp.interrupt();
			clients.forEach(x -> x.interrupt());
			cloud.shutdownNode();
			Thread.sleep(10000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		stats.writeStatisticsToCSVFile("./src/main/resources/simple_data_h.csv", "./src/main/resources/complex_data_h.csv", null);
	}

}
