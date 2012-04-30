import java.util.BitSet;
import java.util.ArrayList;

public class BitMatrix{

	/**
	 * The actual matrix
	 */
	private ArrayList<BitSet> matrix;

	/**
	 * Constructor
	 */
	public BitMatrix(){
		this.matrix = new ArrayList<BitSet>();
		this.matrix.ensureCapacity(10000);
	}

	/**
	 * Constructor
	 * @param N the size of the matrix on one side.
	 */
	public BitMatrix(int N){
		this.matrix = new ArrayList<BitSet>();
		this.matrix.ensureCapacity(N);
	}

	/**
	 * Get
	 */
	public boolean get(int i, int j){
		return matrix.get(j).get(i);
	}

	/**
	 * Set
	 */
	public void put(int i, int j, boolean value){
		while(j>=matrix.size()){
			matrix.add(new BitSet());
		}
		matrix.get(j).set(i, value);
	}

	/**
	 * Count number of vertices set to true.
	 */
	public int numVertices(){
		int total = 0;
		for(BitSet set : matrix){
			total += set.cardinality();
		}
		return total;
	}

	/**
	 * Get the size of a side.
	 */
	public int getN(){
		return matrix.size();
	}

	/**
	 * Get the total size (N x N)
	 */
	public int getGridSize(){
		int side = this.getN();
		return side * side;
	}

}