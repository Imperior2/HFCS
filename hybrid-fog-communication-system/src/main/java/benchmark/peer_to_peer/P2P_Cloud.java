package benchmark.peer_to_peer;

import java.util.HashMap;
import java.util.Map;

import util_objects.Coordinates;
import util_objects.Irreplaceable;
import util_objects.NodeState;

/**
 * Represents the cloud in the peer-to-peer network. This class is only implemented to avoid a cloud node failure.
 * @author Marvin Kruber
 *
 */
public class P2P_Cloud extends P2P_Node implements Irreplaceable {
	
	/** Singleton instance*/
	private static P2P_Cloud singleton = null;

	private P2P_Cloud(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM,
			float SECTOR_DISTANCE, Map<Long, NodeState> clusterMetaData) {
		super(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, SECTOR_DISTANCE, clusterMetaData);
	}
	
	@Override
	public void fail() {
		System.err.println("[WARN] - THE CLOUD DOES NOT FAIL!");
	}
	
	public static P2P_Cloud getInstance(String IP_ADDRESS, int PORT, long NODE_ID, Coordinates COORDINATES, long MAX_STORAGE, long MAX_RAM,
			float SECTOR_DISTANCE) {
		if(singleton == null) {
			singleton = new P2P_Cloud(IP_ADDRESS, PORT, NODE_ID, COORDINATES, MAX_STORAGE, MAX_RAM, SECTOR_DISTANCE, new HashMap<Long, NodeState>());
		}
		return singleton;
	}

}
