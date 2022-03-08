package util_objects;
/**
 * Represents a geographical location.
 * @author Marvin Kruber
 *
 */
public class Coordinates {
	private final float X;
	private final float Y;
	
	/**
	 * @param x - geographical X-coordinate
	 * @param y - geographical Y-coordinate
	 */
	public Coordinates(float x, float y) {
		this.X = x;
		this.Y = y;
	}
	
	/** @return X-coordinate*/
	public float getX() {
		return this.X;
	}
	
	/** @return Y-coordinate*/
	public float getY() {
		return this.Y;
	}
	
	/**
	 * Calculates the distance between the current coordinates and the target.
	 * @param target
	 * @return the distance between the two {@link Coordinates} instances
	 */
	public double getDistance(Coordinates target) {
		 float distX = Math.abs(this.X - target.getX());
		 float distY = Math.abs(this.Y - target.getY());
		 return Math.sqrt((distX * distX) + (distY * distY));
	}
	
	@Override
	public boolean equals(Object object) {
		if(!(object instanceof Coordinates)) {
			return false;
		} else {
			Coordinates coordinates = (Coordinates) object;
			return this.X == coordinates.getX() && this.Y == coordinates.getY();
		}
	}
}
