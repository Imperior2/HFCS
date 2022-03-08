package hybrid;

import java.util.Map;
import java.util.Random;

import hybrid.nodes.Node;
import hybrid.nodes.core_network_layer.CNLNode;
import hybrid.nodes.edge_layer.EdgeNode;

/**
 * The FailureProducer extends {@link Thread} and is used to generate/simulate node failures.
 * @author Marvin Kruber
 *
 */
public class FailureProducer extends Thread {
	
	/** Singleton instance */
	private static FailureProducer singleton = null;
	
	/** Contains all {@link EdgeNode}s of the system. */
	private Map<Long, ? extends Node> allEdgeNodes;
	
	/** Contains all {@link CNLNode}s of the system */
	private Map<Long, ? extends Node> allCNLNodes;
	
	/** Generator for random integer values */
	private Random generator = new Random();
	
	/** {@link Statistics} -> Used to store the number of failed nodes*/
	private Statistics stats = Statistics.getInstance();
	
	/** Indicates the maximum time frame until the next failure occurs [in milliseconds] */
	private final int MAX_TIME_UNTIL_NEXT_FAILURE = 50000;
	
	/** Indicates the minimum time frame until the next failure occurs [in milliseconds] */
	private final int MIN_TIME_UNTIL_NEXT_FAILURE = 5000;
	
	/**
	 * Creates a new FailureProducer
	 * @param edgeNodes - set of all edge nodes
	 * @param cnlNodes - set of all cnl nodes
	 */
	private FailureProducer(Map<Long, ? extends Node> edgeNodes, Map<Long, ? extends Node> cnlNodes) {
		this.allEdgeNodes = edgeNodes;
		this.allCNLNodes = cnlNodes;
	}
	
	@Override
	public void run() {
		int nodeCategoryFactor = 0;
		int nodeSelectionFactor = 0;
		while(!this.isInterrupted()) {
			try {
				Thread.sleep(this.generator.nextInt(MAX_TIME_UNTIL_NEXT_FAILURE) + MIN_TIME_UNTIL_NEXT_FAILURE);
			} catch (InterruptedException e) {
				System.err.println("[INFO] - FAILUREPRODUCER WAS INTERRUPTED");
				this.interrupt();
			}
			nodeCategoryFactor = this.generator.nextInt(100);
			if(nodeCategoryFactor <= 89) { //Marks likelihood for choosing an edge node
				if(allEdgeNodes.size() == 0 ) continue;
				nodeSelectionFactor = this.generator.nextInt(this.allEdgeNodes.size());
				this.chooseFailedNode(nodeSelectionFactor, this.allEdgeNodes).fail();
			} else {
				nodeSelectionFactor = this.generator.nextInt(this.allCNLNodes.size());
				this.chooseFailedNode(nodeSelectionFactor, this.allCNLNodes).fail();
			}
			this.stats.increaseNrOfFailedNodes();
			System.out.println("[INFO] - NODE FAILURE");
		}
		
	}
	
	/**
	 * Chooses a random node out of a given set in order to simulate a failure of this node.
	 * @param counter - random number which determines the failed node
	 * @param allNodes - 
	 * @return the node which should fail
	 */
	private Node chooseFailedNode(int counter, Map<Long, ? extends Node> allNodes) {
		Node failedNode = null;
		int i = 0;
		for(Node n : allNodes.values()) {
			if(i == counter) {
				failedNode = n;
				break;
			}
			i++;
		}
		return failedNode;
	}
	
	/**
	 * Singleton method to get the cloud instance.
	 * @param edgeNodes - set of all {@link EdgeNode}s
	 * @param cnlNodes - set of all {@link CNLNode}s
	 * @return instance of failure producer
	 */
	public static FailureProducer getInstance(Map<Long, ? extends Node> edgeNodes, Map<Long, ? extends Node> cnlNodes) {
		if(singleton == null) singleton = new FailureProducer(edgeNodes, cnlNodes);
		return singleton;
	}
	
}
