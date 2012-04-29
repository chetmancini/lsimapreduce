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

}