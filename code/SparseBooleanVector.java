/**
 * SparseBooleanVector
 * @author chet
 *
 */
public class SparseBooleanVector {
    private final int N;             // length
    private ST<Integer, Boolean> st;  // the vector, represented by index-value pairs

    // initialize the all 0s vector of length N
    public SparseBooleanVector(int N) {
        this.N  = N;
        this.st = new ST<Integer, Boolean>();
    }

    /**
     * Put
     * @param i
     * @param value
     */
    public void put(int i, boolean value) {
        if (i < 0 || i >= N){
        	throw new RuntimeException("Illegal index");
        }
        if (!value){
        	st.delete(i);
        }else{
        	st.put(i, value);
        }
    }

    /**
     * Get
     * @param i
     * @return
     */
    public boolean get(int i) {
        if (i < 0 || i >= N) throw new RuntimeException("Illegal index");
        if (st.contains(i)){
        	return st.get(i);
        }else{
        	return false;
        }
    }

    /**
     * 'True' entry count
     * @return
     */
    public int nnz() {
        return st.size();
    }

    /**
     * Size of vector (all values)
     * @return
     */
    public int size() {
        return N;
    }

    // return a string representation
    public String toString() {
        String s = "";
        for (int i : st) {
            s += "(" + i + ", " + st.get(i) + ") ";
        }
        return s;
    }

}