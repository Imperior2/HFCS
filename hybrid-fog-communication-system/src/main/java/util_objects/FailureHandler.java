package util_objects;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import hybrid.nodes.Node;

/**
 * This interface is used to solve node failure by starting a specific failure routine {@link FailureHandler#startFailureRoutine(Node, Long)}
 * @author Marvin Kruber
 *
 */
public interface FailureHandler {

	/** This map stores the NODE_ID of a failed nodes as key and the NODE_IDs of the reporters as values.  */
	public static Map<Long, Set<Long>> reportedNodeFailures = new HashMap<Long, Set<Long>>();
	
	/**
	 * Start a failure routine to keep the system running.
	 * @param failedNode - node which potentially failed
	 * @param NodeID -  ID of the reporting node
	 */
	public void startFailureRoutine(Node failedNode, Long NodeID);
}
