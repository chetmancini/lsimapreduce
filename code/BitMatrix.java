import java.util.BitSet;

public class BitMatrix{

	private BitSet[] matrix;

	public BitMatrix(int N){
		this.matrix = new BitSet[N]
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
	public void set(int i, int j){
		matrix[j].set(i)
	}

}