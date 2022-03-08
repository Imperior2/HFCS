package util_objects;

import java.util.HashMap;
import java.util.Map;

import hybrid.nodes.Node;

/**
 * Wrapper class which contains the metadata information of a node.
 * Source: <a href="https://martinfowler.com/articles/patterns-of-distributed-systems/gossip-dissemination.html">
 * https://martinfowler.com/articles/patterns-of-distributed-systems/gossip-dissemination.html</a><br>
 * Accessed: 02/02/2022
 * @author Marvin Kruber
 */
public class NodeState {
	
	/** Stores all metadata information fields and their corresponding value versions ({@link VersionedValue}) of a node. */
	private Map<String, VersionedValue<?>> metaDataInf = new HashMap<String, VersionedValue<?>>();
	
	/** Represents the associated node. */
	private Node node;
	
	/** Creates a new NodeState. */
	public NodeState(Node node) {
		this.node = node;
	}
	
	/**
	 * Creates a copy of the node state. This is used to avoid that all nodes only hold the same reference instead of
	 * the values. If all nodes hold the same reference, changes of the node state would be propagated immediately. This would
	 * not simulate the real world scenario. Hence, the node state has to newly created.
	 * @param nodeState
	 */
	public NodeState(NodeState nodeState) {
		this.node = nodeState.getAssociatedNode();
		this.metaDataInf = new HashMap<>(nodeState.getFields());
	}
	
	/**
	 * Updates the map of metadata information ({@link NodeState#metaDataInf}) of the node.
	 * @param field - field which should be inserted or updated
	 * @param value - new value version
	 */
	public void updateMetaData(String field, VersionedValue<?> value) {
		this.metaDataInf.put(field, value);
	}
	
	//================================  Getter   ===============================================
	
		/** @return map of all fields and their current value versions ({@link NodeState#metaDataInf}).*/
		public Map<String, VersionedValue<?>> getFields() {
			return this.metaDataInf;
		}
		
		/** @return value version of the specified field */
		public VersionedValue<?> getField(String field){
			return this.metaDataInf.get(field);
		}
		
		/** @return associated node */
		public Node getAssociatedNode(){
			return this.node;
		}
}
