import java.util.BitSet;

public class BitMatrix{

	/**
	 * The actual matrix
	 */
	private BitSet[] matrix;

	/**
	 * Constructor
	 */
	public BitMatrix(int N){
		this.matrix = new BitSet[N];
	}

	/**
	 * Get
	 */
	public boolean get(int i, int j){
		return matrix[j].get(i);
	}

	/**
	 * Set
	 */
	public void put(int i, int j, boolean value){
		matrix[j].set(i, value);
	}

}