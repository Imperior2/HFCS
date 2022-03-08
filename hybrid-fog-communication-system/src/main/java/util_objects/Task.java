package util_objects;

/**
 * Represents a task.
 * @author Marvin Kruber
 *
 */
public class Task {
	
	/** Storage capacity required for the task [in byte] */
	private final int requiredStorage;
	
	/** Computation capacity required for the task [in byte] */
	private final int requiredRAM;
	
	/** Start time [in milliseconds] */
	private final long startTime = System.currentTimeMillis();
	
	/** Finish time [in milliseconds] */
	private long finishTime;
	
	/**
	 * Creates a new {@link Task}.
	 * @param requiredStorage - storage capacity required for the task [in byte]
	 * @param requiredRAM - computation capacity required for the task [in byte]
	 */
	public Task(int requiredStorage, int requiredRAM) {
		this.requiredStorage = requiredStorage;
		this.requiredRAM = requiredRAM;
	}
	
	public void finishTask() {
		this.finishTime = System.currentTimeMillis();
	}
	
	//================================  Getter   ===============================================
	
	/** @return required storage capacity*/
	public int getRequiredStorage() {
		return this.requiredStorage;
	}
	
	/** @return required computation capacity*/
	public int getRequiredRAM() {
		return this.requiredRAM;
	}
	
	/** @return execution time of the task*/
	public long getExecutionTime() {
		return this.requiredRAM + this.requiredStorage;
	}
	
	/** @return transmission delay*/
	public long getTransmissionDelay() { // *2 because of the transmission from and to the client
		return 2 * ((this.finishTime - this.startTime) - this.getExecutionTime());
	}
}
