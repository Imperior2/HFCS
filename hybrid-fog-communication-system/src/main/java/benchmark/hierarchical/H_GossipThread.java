package benchmark.hierarchical;

import java.util.Map;
import java.util.NoSuchElementException;

import hybrid.nodes.GossipThread;
import hybrid.nodes.Node;
import util_objects.NodeState;

/** Hierarchical version of {@link GossipThread}*/
public class H_GossipThread extends GossipThread{
	
	/** Supervisor which receives the {@link NodeState} updates of this node*/
	private Node supervisor;

	/**
	 * Creates a new gossip thread for the hierarchical communication
	 * @param server - associated node
	 * @param clusterMetaData - map to store metadata information 
	 * @param supervisor - supervisor of the associated node
	 */
	public H_GossipThread(Node server, Map<Long, NodeState> clusterMetaData, Node supervisor) {
		super(server, clusterMetaData);
		this.supervisor = supervisor;
	}
	
	@Override
	public void run() {
		while(!this.isInterrupted()) {
			try {
				this.sendGossipToSupervisor();
				Thread.sleep(gossipInterval);
			} catch(InterruptedException e) {
				System.err.println("[WARN] - H_GOSSIP WAS INTERRUPTED! NODE: " + this.server.getNodeID());
				
				if(!this.server.isAvailable()) {//If associated node failed
					System.out.println("FAILED NODE" + this.server.getNodeID());
					this.server.reportNodeFailureToSupervisor(this.server, this.supervisor.getNodeID());
				}
				this.interrupt();
			}
		}
	}
	
	/**
	 * Sends own metadata information to the supervisor. If the supervisor failed, the node shuts down. 
	 */
	private void sendGossipToSupervisor() {
		try {
			this.server.increaseMsgSent();
			this.supervisor.receiveAndRespondGossipFrom(this.server, this.clusterMetaData);
		} catch(NoSuchElementException e) {//If supervisor failed
			System.err.println(e.getMessage());
			if(supervisor instanceof H_CNLNode) {
				System.err.print("[WARN] - NODE: " + this.server.getNodeID() + " IS UNAVAILABLE. SHUT DOWN NODE.");
				this.server.shutdownNode();
			} else {
				throw new RuntimeException("THE CLOUD COULD NOT FAIL");
			}
		}
		
	}
	
	/** Replaces the supervisor with newSupervisor.*/
	public void changeSupervisor(Node newSupervisor) {
		synchronized(this.supervisor) {
			this.supervisor = newSupervisor;
		}
	}

}
