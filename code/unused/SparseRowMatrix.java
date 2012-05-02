package unused;

/*************************************************************************
 *  A sparse, square matrix, implementing using two arrays of sparse
 *  vectors, one representation for the rows and one for the columns.
 *
 * 
 * Copyright © 2000–2011, Robert Sedgewick and Kevin Wayne. 
 *
 *************************************************************************/
/**
 * Adapted from Robert Sedgewick and Kevin Wayne
 * @author chet
 */
public class SparseRowMatrix {
    private final int N;           // N-by-N matrix
    private SparseVector[] rows;   // the rows, each row is a sparse vector

    // initialize an N-by-N matrix of all 0s
    public SparseRowMatrix(int N) {
        this.N  = N;
        rows = new SparseVector[N];
        for (int i = 0; i < N; i++) rows[i] = new SparseVector(N);
    }

    // put A[i][j] = value
    public void put(int i, int j, double value) {
        if ((i < 0 || i >= N) || (j < 0 || j >= N)){
            throw new RuntimeException("Illegal index");
        }
        rows[i].put(j, value);
    }

    // return A[i][j]
    public double get(int i, int j) {
        if ((i < 0 || i >= N) || (j < 0 || j >= N)){
            throw new RuntimeException("Illegal index");
        }
        return rows[i].get(j);
    }

    // return the number of nonzero entries (not the most efficient implementation)
    public int nnz() { 
        int sum = 0;
        for (int i = 0; i < N; i++)
            sum += rows[i].nnz();
        return sum;
    }

    // return the matrix-vector product b = Ax
    public SparseVector times(SparseVector x) {
        SparseRowMatrix A = this;
        if (N != x.size()) throw new RuntimeException("Dimensions disagree");
        SparseVector b = new SparseVector(N);
        for (int i = 0; i < N; i++)
            b.put(i, A.rows[i].dot(x));
        return b;
    }

    // return C = A + B
    public SparseRowMatrix plus(SparseRowMatrix B) {
    	SparseRowMatrix A = this;
        if (A.N != B.N) throw new RuntimeException("Dimensions disagree");
        SparseRowMatrix C = new SparseRowMatrix(N);
        for (int i = 0; i < N; i++)
            C.rows[i] = A.rows[i].plus(B.rows[i]);
        return C;
    }


    // return a string representation
    public String toString() {
        String s = "N = " + N + ", nonzeros = " + nnz() + "\n";
        for (int i = 0; i < N; i++) {
            s += i + ": " + rows[i] + "\n";
        }
        return s;
    }
}