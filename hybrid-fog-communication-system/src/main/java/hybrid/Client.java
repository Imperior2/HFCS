package hybrid;

import java.util.Random;

import hybrid.nodes.Node;
import util_objects.Coordinates;
import util_objects.Irreplaceable;
import util_objects.Task;

/**
 * This class represents a mobile client. Clients are able to move around the world and send tasks to the system. <br>
 * It extends {@link Thread}.
 * @author Marvin Kruber
 *
 */
public class Client extends Thread {
	
	/** Maximum value for geographical longitude -> Marks a movement restriction */ 
	private final float MAX_X_COORDINATE = 180;
	
	/** Minimum value for geographical longitude -> Marks a movement restriction */ 
	private final float MIN_X_COORDINATE = -180;
	
	/** Maximum value for geographical latitude -> Marks a movement restriction */
	private final float MAX_Y_COORDINATE = 90;
	
	/** Minimum value for geographical latitude -> Marks a movement restriction */
	private final float MIN_Y_COORDINATE = -90;
	
	/** Represents the maximum number of steps a client could take */
	private final int MAX_MOVEMENT = 2;
	
	/** Current position of the client */
	private Coordinates currentPosition;
	
	/** Represents the maximum capacity requirements of a task */
	private final int MAX_CAPACITY_REQ = 5000;
	
	/** Represents the minimum capacity requirements of a task */
	private final int MIN_CAPACITY_REQ = 1000;
	
	/** Generator for random integer values */
	private Random generator = new Random();
	
	/** Receives all requests from the client */
	private Node contactNode;
	
	/** The cloud is the emergency contact if a node is unavailable*/
	private Irreplaceable cloud;
	
	/** Number of requests which were send by the client*/
	private int numberOfRequests = 0;
	
	/** {@link Statistics}*/
	private Statistics stats = Statistics.getInstance();
	
	/**
	 * Creates a new {@link Client} which is allocated at initialPosition.
	 * @param initialPosition - start position of the client
	 * @param initialContactNode - the cloud of the prototype
	 */
	public Client(Coordinates initialPosition, Irreplaceable initialContactNode) {
		this.currentPosition = initialPosition;
		this.cloud = initialContactNode;
		this.contactNode = (Node) initialContactNode;
	}
	
	@Override
	public void run() {
		while(!this.isInterrupted()) {
			try {
				Thread.sleep(1000 + this.generator.nextInt(5000));
				//Send Task
				//System.out.println("[CLIENT] - SEND TASK");
				Task task = new Task (this.generator.nextInt(MAX_CAPACITY_REQ) + MIN_CAPACITY_REQ, 
						this.generator.nextInt(MAX_CAPACITY_REQ) + MIN_CAPACITY_REQ); 
				this.checkNodeAvailability();
				this.contactNode.receiveTaskFromClient(task, this);
				this.numberOfRequests++;
				move();
			} catch (InterruptedException e) {
				System.err.println("[INFO] - CLIENT WAS INTERRUPTED");
				this.interrupt();
			}
		}
		stats.increaseTotalNrOfTasks(this.numberOfRequests);
	}
	
	/** Simulates the movement of a client on the globe. */
	private void move() {
		//Generates the movement directions randomly
		int xMovementDirection = (this.generator.nextBoolean()) ? -1 : 1; //Generates the movement directions at random
		int yMovementDirection = (this.generator.nextBoolean()) ? -1 : 1;
		//Calculate the concrete number of movement steps
		float nextXMovement = xMovementDirection * this.generator.nextFloat() * MAX_MOVEMENT;
		float nextYMovement = yMovementDirection * this.generator.nextFloat() * MAX_MOVEMENT;
		
		float newX = this.currentPosition.getX() + nextXMovement;
		float newY = this.currentPosition.getX() + nextYMovement;
		
		//Check if X borders are exceeded and adjust the position if necessary => Maps the date line
		if(newX > MAX_X_COORDINATE) {
			newX = MIN_X_COORDINATE + (newX - MAX_X_COORDINATE);
		} else if(newX < MIN_X_COORDINATE) {
			newX = MAX_X_COORDINATE + (newX - MIN_X_COORDINATE);
		}
		
		//Check if Y borders are exceeded and adjust the position if necessary => Maps the poles
		if (newY > MAX_Y_COORDINATE) {
			newY = MAX_Y_COORDINATE - (newY - MAX_Y_COORDINATE);
		} else if(newY < MIN_Y_COORDINATE) {
			newY = MIN_Y_COORDINATE - (newY - MIN_Y_COORDINATE);
		}
		this.currentPosition = new Coordinates(newX, newY);
	}
	
	/**
	 * Replaces {@link Client#contactNode} with newContactNode.
	 * @param newContactNode - node which should receive request in future
	 */
	public void changeContactNode(Node newContactNode) {
		synchronized(this.contactNode) {
			this.contactNode = newContactNode;
		}
	}
	
	/** @return position of the client*/
	public Coordinates getClientPosition() {
		return this.currentPosition;
	}
	
	/** Checks whether a node is available. If not it contacts the cloud in order to be reassigned. <br>
	 * This also simulates connection issues of the client.*/
	private void checkNodeAvailability() {
		if(!this.contactNode.isAvailable()) {
			this.changeContactNode((Node) this.cloud);
		}
	}
	
}
