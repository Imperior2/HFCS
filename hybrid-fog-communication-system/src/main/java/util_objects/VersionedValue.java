package util_objects;

/**
 * Wrapper class for a specific value, which simultaneously indicates its version. The version parameter is used for the 
 * implementation of a lamport clock. If a value with a higher version is received the old value can be discarded. <br>
 * Source: <a href="https://martinfowler.com/articles/patterns-of-distributed-systems/gossip-dissemination.html">
 * https://martinfowler.com/articles/patterns-of-distributed-systems/gossip-dissemination.html</a><br>
 * Accessed: 02/02/2022
 * @author Marvin Kruber
 * @param <T> Value type
 */
public class VersionedValue<T> {
	
	/** Version of the value */
	private long version;
	
	/** Value */
	private T value;
	
	/**
	 * Creates a new value version ({@link VersionedValue}).
	 * @param value
	 * @param version
	 */
	public VersionedValue(T value, long version) {
		this.version = version;
		this.value = value;
	}
	
	//================================  Getter   ===============================================
	
	/** @return version of the current value */
	public long getVersion() {
		return this.version;
	}
	
	/** @return value*/
	public T getValue() {
		return this.value;
	}
}
