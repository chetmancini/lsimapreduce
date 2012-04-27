/*************************************************************************
 *  Compilation:  javac SparseMatrix.java
 *  Execution:    java SparseMatrix
 *  
 *  A sparse, square matrix, implementing using two arrays of sparse
 *  vectors, one representation for the rows and one for the columns.
 *
 *  For matrix-matrix product, we might also want to store the
 *  column representation.
 * 
 * Copyright © 2000–2011, Robert Sedgewick and Kevin Wayne. 
 *
 *************************************************************************/
import java.util.ArrayList;
import java.util.List;

public class SparseColumnMatrix {
    private final int N;           // N-by-N matrix
    private int width;
    private SparseVector[] cols;   // the rows, each row is a sparse vector

    // initialize an N-by-N matrix of all 0s
    public SparseColumnMatrix(int N) {
        this.N  = N;
        this.width = N;
        cols = new SparseVector[N];
        for (int i = 0; i < N; i++) cols[i] = new SparseVector(N);
    }

    /**
     * Create a scm with existing columns
     */
    public SparseColumnMatrix(int N, SparseVector[] cols){
        this.N = N;
        this.width = cols.length;
        this.cols = cols;
    }

    /**
     * Break into column sets.
     * TODO: finish
     */
    public SparseColumnMatrix slice(int startCol, int endCol){
    	return null;
    }

    /**
     * Split a matrix into columns
     */
    public List<SparseColumnMatrix> split(int parts){
        ArrayList<SparseColumnMatrix> toReturn = new ArrayList<SparseColumnMatrix>();
        int subwidth = this.width / parts;
        int remainder = this.width % parts;
        for(int i=0;i<parts;i++){

        }
        return toReturn;
    }

    /**
     * i=row
     * j=column
     */
    public void put(int i, int j, double value) {
        if (i < 0 || i >= N) throw new RuntimeException("Illegal index");
        if (j < 0 || j >= N) throw new RuntimeException("Illegal index");
        cols[j].put(i, value);
    }

    /**
     *i=row
     * j=col
     */
    public double get(int i, int j) {
        if (i < 0 || i >= N) throw new RuntimeException("Illegal index");
        if (j < 0 || j >= N) throw new RuntimeException("Illegal index");
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

    // return the matrix-vector product b = Ax
    public SparseVector times(SparseVector x) {
        return null;
        // SparseColumnMatrix A = this;
        // if (N != x.size()) throw new RuntimeException("Dimensions disagree");
        // SparseVector b = new SparseVector(N);
        // for (int i = 0; i < N; i++)
        //     b.put(i, A.rows[i].dot(x));
        // return b;
    }

    // return C = A + B
    public SparseColumnMatrix plus(SparseColumnMatrix B) {
        return null;
        // SparseColumnMatrix A = this;
        // if (A.N != B.N) throw new RuntimeException("Dimensions disagree");
        // SparseColumnMatrix C = new SparseColumnMatrix(N);
        // for (int i = 0; i < N; i++)
        //     C.rows[i] = A.rows[i].plus(B.rows[i]);
        // return C;
    }


    // return a string representation
    public String toString() {
        return "Not implemented";
        // String s = "N = " + N + ", nonzeros = " + nnz() + "\n";
        // for (int i = 0; i < N; i++) {
        //     s += i + ": " + rows[i] + "\n";
        // }
        // return s;
    }


    // test client
    public static void main(String[] args) {
        SparseColumnMatrix A = new SparseColumnMatrix(5);
        SparseVector x = new SparseVector(5);
        A.put(0, 0, 1.0);
        A.put(1, 1, 1.0);
        A.put(2, 2, 1.0);
        A.put(3, 3, 1.0);
        A.put(4, 4, 1.0);
        A.put(2, 4, 0.3);
        x.put(0, 0.75);
        x.put(2, 0.11);
        System.out.println("not implemented");
        // System.out.println("x     : " + x);
        // System.out.println("A     : " + A);
        // System.out.println("Ax    : " + A.times(x));
        // System.out.println("A + A : " + A.plus(A));
    }
}