package hybrid.nodes;

import util_objects.Task;

/**
 * Represents a thread of a node. It extends {@link Thread}.
 * WorkerThread is used to process a task.
 * @author Marvin Kruber
 *
 */
public class WorkerThread extends Thread {
	/** Represents the associated server */
	private Node server;
	
	/** Represents the task being performed */
	private Task task;
	
	/**
	 * Creates a new {@link WorkerThread} of a server for a specific task.
	 * @param server
	 * @param task
	 */
	public WorkerThread(Node server, Task task) {
		this.server = server;
		this.task = task;
	}
	
	@Override
	public void run() {
		try {
			//System.out.println("[INFO] - TASK ACCEPTED BY:" + this.server.getNodeID());
			
			Thread.sleep(this.task.getRequiredRAM()); //TODO BOOSTER?
			//System.out.println("[INFO] - APPLICATION DATA PROCESSED");
			
			Thread.sleep(this.task.getRequiredStorage());
			//System.out.println("[INFO] - APPLICATION DATA STORED");
			
			this.server.completeTask(task); //Completes task and releases capacities
		} catch (InterruptedException e) {
			System.err.println("[INFO] - TASK INTERRUPTED. NODE: " + this.server.getNodeID());
		}
		
	}
}
