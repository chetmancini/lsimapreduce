import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Sparse Column Matrix
 * @author Chet Mancini
 *
 */
public class SparseColumnMatrix {
    private final int N;           // N-by-N matrix
    private int width;
    private SparseBooleanVector[] cols;   // the rows, each row is a sparse vector

    /**
     * Initialize
     * @param N
     */
    public SparseColumnMatrix(int N) {
        this.N  = N;
        this.width = N;
        cols = new SparseBooleanVector[N];
        for (int i = 0; i < N; i++){
            cols[i] = new SparseBooleanVector(N);
        }
    }

    /**
     * Create a scm with existing columns
     */
    public SparseColumnMatrix(int N, SparseBooleanVector[] cols){
        this.N = N;
        this.width = cols.length;
        this.cols = cols;
    }

    /**
     * Break into column sets.
     * @param startCol the beginning col, inclusive.
     * @param endCol the end column, exclusive.
     * Note: endcol may lie outside the bound, in which case it will
     * take the largest possible subarray. 
     */
    public SparseColumnMatrix slice(int startCol, int endCol) throws Exception{
      if (startCol >= endCol){
        throw new Exception("bad indicies");
      }
      SparseBooleanVector[] toReturn = Arrays.copyOfRange(this.cols, startCol, endCol);
      return new SparseColumnMatrix(this.N, toReturn);
    }

    /**
     * Split a matrix into columns
     */
    public List<SparseColumnMatrix> split(int parts) throws Exception{
        ArrayList<SparseColumnMatrix> toReturn = new ArrayList<SparseColumnMatrix>();
        int subwidth = this.width / parts;
        for(int i=0;i<parts;i++){
          toReturn.add(this.slice(subwidth*i, subwidth*(i+1)));
        }
        return toReturn;
    }

    /**
     * i=row
     * j=column
     */
    public void put(int i, int j, boolean value) {
        if (i < 0 || i >= N){
        	throw new RuntimeException("Illegal index");
        }
        if (j < 0 || j >= N){
        	throw new RuntimeException("Illegal index");
        }
        cols[j].put(i, value);
    }

    /**
     *i=row
     * j=col
     */
    public boolean get(int i, int j) {
        if (i < 0 || i >= N){
        	throw new RuntimeException("Illegal index");
        }
        if (j < 0 || j >= N){
        	throw new RuntimeException("Illegal index");
        }
        return cols[j].get(i);
    }

    /**
     * return the number of nonzero entries (not the most efficient implementation)
     */ 
    public int nnz() { 
        int sum = 0;
        for (int i = 0; i < N; i++)
            sum += cols[i].nnz();
        return sum;
    }

    /**
     * Return a string representation.
     */
    public String toString() {
        return "Not implemented";
    }
    
}
