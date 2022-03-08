package hybrid;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import util_objects.Coordinates;

/**
 * After terminating the system, this class contains all relevant statistics and write them to CSV-files.
 * @author Marvin Kruber
 *
 */
public class Statistics {

	/** Singleton instance */
	private static Statistics singleton = null;
	
	/** Number of {@link EdgeNodes} */
	private int nr_Of_Edge_Nodes = 0; 
	
	/** Number of {@link CNLNodes} */ 
	private int nr_Of_CNL_Nodes = 0;
	
	/** Number of {@link Cloud} */ //This is just mentioned as reminder
	private final int nr_Of_Cloud = 1; 
	
	/** Number of failed nodes */
	private int nr_Of_Failed_Nodes = 0; 
	
	/** Number of detected node failures */
	private int nr_Of_Detected_Node_Failures = 0;
	
	/** Total number of tasks sent by all clients */
	private int total_Nr_Of_Tasks = 0;
	
	/** Number of completed tasks */
	private int nr_Of_Completed_Tasks = 0; 
	
	/** Number of escalated tasks */
	private int nr_Of_Escalated_Tasks = 0; 
	
	/** Number of redirected tasks */
	private int nr_Of_Redirected_Tasks = 0; 
	
	/** Contains the coordinates of all nodes */
	private Map<Long, Coordinates> coordinates_Of_Nodes = new HashMap<>();
	
	/** Contains the failure state of each node */
	private Map<Long, Boolean> node_Failures = new HashMap<>();
	
	/** Contains the number of solved tasks for each node */
	private Map<Long, Integer> number_Of_Tasks_Per_Node = new HashMap<>();
	
	/** Contains the avg_transmission_delay per node */
	private Map<Long, Double> avg_transmission_latency = new HashMap<>();
	
	/** Contains the avg_execution_time per node */
	private Map<Long, Double> avg_execution_time = new HashMap<>();
	
	/** Contains the number of sent gossip messages per node */
	private Map<Long, Integer> number_Of_Sent_Gossip_Messages = new HashMap<>();
	
	/** Contains the number of received gossip messages per node */
	private Map<Long, Integer> number_Of_Received_Gossip_Messages = new HashMap<>();
	
	/** Contains the number of replied gossip messages per node */
	private Map<Long, Integer> number_Of_Replied_Gossip_Messages = new HashMap<>();
	
	/** Contains the information which edge node belongs to which edge pool */
	private Map<Long, Integer> edgePoolMembership = new HashMap<>();
	
	/** Contains the IDs of all edge pools, their sub pool and their edge pool members per {@link CNLNode} */
	private Map<Long ,Map<Integer, Set<Long>>> edge_Pool_Member_IDs = new HashMap<>();
	
	/** Contains the number of edge pools per {@link CNLNode} */
	private Map<Long, Integer> number_Of_EdgePools = new HashMap<>();
	
	private Statistics() {
	}
	
	//------------------------------------------- Access methods ----------------------------------------------------------------------
	
	/**
	 * Singleton method to get the statistics instance.
	 * @return statistics instance
	 */
	public static Statistics getInstance() {
		if(singleton == null) singleton = new Statistics();
		return singleton;
	}
	
	/** Increases the number of {@link EdgeNode}s */
	public synchronized void increaseNrOfEdgeNodes() {
		this.nr_Of_Edge_Nodes++;
	}
	
	/** Increases the number of {@link CNLNode}s */
	public synchronized void increaseNrOfCNLNodes() {
		this.nr_Of_CNL_Nodes++;
	}
	
	/** Increases the number of failed nodes */
	public synchronized void increaseNrOfFailedNodes() {
		this.nr_Of_Failed_Nodes++;
	}
	
	/** Increases the number of detected node failures*/
	public synchronized void increaseNrOfDetectedNodeFailures() {
		this.nr_Of_Detected_Node_Failures++;;
	}
	
	/** Increases the number of escalated tasks */
	public synchronized void increaseTotalNrOfTasks(int number) {
		this.total_Nr_Of_Tasks += number;
	}
	
	/** Increases the number of escalated tasks */
	public synchronized void increaseNrOfEscalatedTask() {
		this.nr_Of_Escalated_Tasks++;
	}
	
	/** Increases the number of redirected tasks */
	public synchronized void increaseNrOfRedirectedTasks() {
		this.nr_Of_Redirected_Tasks++;
	}
	
	/** Stores the coordinates of a node.*/
	public synchronized void storeCoordinates(Long NodeID, Coordinates coordinates) {
		this.coordinates_Of_Nodes.put(NodeID, coordinates);
	}
	
	/** Stores the failure state of a node.*/
	public synchronized void storeFailureState(long node_ID, boolean hasFailed) {
		this.node_Failures.put(node_ID,hasFailed);
	}
	
	/** Stores the average transmission latency of a node.*/
	public synchronized void storeAvgTransmisionLatency(Long NodeID, Double avgLatency) {
		this.avg_transmission_latency.put(NodeID, avgLatency);
	}
	
	/** Stores the average execution time of a node.*/
	public synchronized void storeAvgExecutionTime(Long NodeID, Double avgExecutionTime) {
		this.avg_execution_time.put(NodeID, avgExecutionTime);
	}
	
	/** Stores the number of tasks solved per node.*/
	public synchronized void storeNumberOfTasksPerNode(Long NodeID, Integer nrOfTasks) {
		this.number_Of_Tasks_Per_Node.put(NodeID, nrOfTasks);
	}
	
	/** Stores the number of gossip messages sent by a node.*/
	public synchronized void storeNumberOfGossipMsgSent(Long NodeID, Integer nrOfGossipMsgSent) {
		this.number_Of_Sent_Gossip_Messages.put(NodeID, nrOfGossipMsgSent);
	}
	
	/** Stores the number of gossip messages received by a node.*/
	public synchronized void storeNumberOfGossipMsgReceived(Long NodeID, Integer nrOfGossipMsgReceived) {
		this.number_Of_Received_Gossip_Messages.put(NodeID, nrOfGossipMsgReceived);
	}
	
	/** Stores the number of gossip messages replied by a node.*/
	public synchronized void storeNumberOfGossipMsgReplied(Long NodeID, Integer nrOfGossipMsgReplied) {
		this.number_Of_Replied_Gossip_Messages.put(NodeID, nrOfGossipMsgReplied);
	}
	
	/** Stores the edge pool membership of an {@link EdgeNode}*/
	public synchronized void storeEdgePoolMembership(Long NodeID, Integer edgePoolID) {
		this.edgePoolMembership.put(NodeID, edgePoolID);
	}
	
	/** Stores the IDs of all edge pools and their members per {@link CNLNode}*/
	public synchronized void storeIDsOfEdgePoolMembers(Long nodeID, Map<Integer, Set<Long>> allMemberIDs) {
		this.edge_Pool_Member_IDs.put(nodeID , allMemberIDs);
	}
	
	/** Stores the number of edge pools of a {@link CNLNode}*/
	public synchronized void storeNumberOfEdgePools(Long NodeID, Integer nrOfEdgePools) {
		this.number_Of_EdgePools.put(NodeID, nrOfEdgePools);
	}
	
	//------------------------------------------- Write to files ----------------------------------------------------------------------
	
	/** Writes all statistics to various .csv-files. 
	 * @param simpleDataPath - path to the file in which the simple statistics should be stored
	 * @param complexDataPath - path to the file in which the complex statistics should be stored
	 * @param edgePoolStatsPath - path to the file in which the edge pool statistics should be stored (if null is passed this is skipped)
	 * */ 
	public void writeStatisticsToCSVFile(String simpleDataPath, String complexDataPath, String edgePoolStatsPath) {
		System.out.println("[INFO] - WRITE STATISTICS TO FILE");
		BufferedWriter writer;
		try {
			writer = new BufferedWriter(new FileWriter(new File(simpleDataPath), true));
			writer.write(this.getAllVariablesAsString());
			writer.close();
			
			writer = new BufferedWriter(new FileWriter(new File(complexDataPath), true));
			writer.write(this.getMapsAsString());
			writer.close();
			
			if(!(edgePoolStatsPath == null)) {
				writer = new BufferedWriter(new FileWriter(new File(edgePoolStatsPath), true));
				writer.write(this.getEdgePoolStatisticsAsString());
				writer.close();
			}
		
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("[INFO] - FINISHED WRITING");
	}
	
	//------------------------------------------- String formats -------------------------------------------------------------------------
	
	/** @return string of all variable values*/
	private String getAllVariablesAsString() {
		StringBuilder sb = new StringBuilder();
		this.nr_Of_Completed_Tasks = this.number_Of_Tasks_Per_Node.values().stream().reduce(0, (acc, y) -> acc + y);
		sb.append(this.nr_Of_Cloud + ";");
		sb.append(this.nr_Of_CNL_Nodes + ";");
		sb.append(this.nr_Of_Edge_Nodes + ";");
		sb.append(this.nr_Of_Failed_Nodes + ";");
		sb.append(this.nr_Of_Detected_Node_Failures + ";");
		sb.append(this.total_Nr_Of_Tasks + ";");
		sb.append(this.nr_Of_Completed_Tasks + ";");
		sb.append(this.nr_Of_Redirected_Tasks + ";"); //For sufficiently large pools or many nodes redirected tasks > escalated tasks
		sb.append(this.nr_Of_Escalated_Tasks + "\n");
		return sb.toString();
	}
	
	/** @return string of all maps (except nr_Of_Edge_Pools)*/
	private String getMapsAsString() {
		StringBuilder sb = new StringBuilder();
		for(Entry<Long, Integer> e : this.number_Of_Sent_Gossip_Messages.entrySet()) {
			sb.append(e.getKey()+ ";" + this.edgePoolMembership.get(e.getKey()) + ";"); //NODE_ID and POOL_ID
			Coordinates coordinates = this.coordinates_Of_Nodes.get(e.getKey());
			sb.append(coordinates.getX() + ";" + coordinates.getY() + ";");				//COORDINATES
			sb.append(this.node_Failures.get(e.getKey()) + ";");
			sb.append(e.getValue() + ";");												//SENT_GOSSIP_MESSAGES
			sb.append(this.number_Of_Received_Gossip_Messages.get(e.getKey()) + ";");	//RECEIVED_GOSSIP_MESSAGES
			sb.append(this.number_Of_Replied_Gossip_Messages.get(e.getKey()) + ";");	//REPLIED_GOSSIP_MESSAGES
			sb.append(this.number_Of_Tasks_Per_Node.get(e.getKey()) + ";");				//NUMBER_OF_TASKS_PER_NODE
			sb.append(this.avg_execution_time.get(e.getKey()) + ";");					//AVG_EXECUTION_TIME_PER_NODE
			sb.append(this.avg_transmission_latency.get(e.getKey()) + "\n");			//AVG_TRANSMISSION_LATENCY_PER_NODE
		}
		return sb.toString();
	}
	
	/** @return string representation of {@link Statistics#number_Of_EdgePools} and {@link Statistics#edgePoolMembership}*/
	private String getEdgePoolStatisticsAsString() {
		StringBuilder sb = new StringBuilder();
		for(Entry<Long, Integer> cnlNode : this.number_Of_EdgePools.entrySet()) {
			sb.append(cnlNode.getKey() + ";" + cnlNode.getValue() + ";");
			for (Entry<Integer, Set<Long>> pool : this.edge_Pool_Member_IDs.get(cnlNode.getKey()).entrySet()) {
				sb.append(pool.getKey() + ";" + pool.getValue() + ";");
			}
			sb.append("\n");
		}
		return sb.toString();
	}
}
