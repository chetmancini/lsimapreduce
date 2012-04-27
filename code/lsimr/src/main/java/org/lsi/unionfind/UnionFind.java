package org.lsi.unionfind;
/**
 * Disclaimer: this is an untested work in
 * progress.  Pushed only for backup and transfer
 * to laptop.  Please don't try this at home.
 * 
 * @author sean
 *
 */

public class UnionFind {

	/**
	 * id is an array of roots for each
	 * point.  As the union find algorithm
	 * runs, it updates the id vector until
	 * each set of connected components has 
	 * the same root.
	 */
	private int[] m_id;
    private int m = 0;
    private int n = 0;
    private int m_edges = 0;
	
	/**
	 * Data structure to efficiently keep track of
	 * connected components.
     * @param id a vector with 0 if there is no tree,
     *           and the id of the current root if there
     *           is a tree.  Current root is the index
     *           of the tree initially.
	 * @param n  the number of columns in the original
     *           matrix.
     * @param m  the number of rows in the original matrix.
	 */
	public UnionFind(int[] id, int m, int n)
	{
		this.m_id = id;
        this.m = m;
        this.n = n;
        this.m_edges = 0;

        //This looks redundant, but we look both ways even
        //if left is clearly lower, just so that we can also
        //count the vertices as we go.
        for(int j=0; j<2; ++j)
        for(int i=0; i<m_id.length; ++i)
        {
            if(m_id[i] > 0)
            {
                lookUp(i);
                lookRight(i);
            }
        };

        //TODO: This is correct but sloppy.  We double count
        //every vertex, so have to devide by two here.
        
        m_edges = m_edges/2;
	}
	
    public int[] getRoots(){
        return m_id;
    }

    public int getEdges(){
        return m_edges;
    }

    /**
     * Return the id of the element to the left, if there
     * is a tree there, otherwise return own id.
     * @param i The index of the point of interest.
     */
    private void lookRight(int i)
    {
        //if i'm not the right column, and the spot right of me
        //has a tree.
        if(i+m < m_id.length && m_id[i+m] > 0){
            ++m_edges;  //there is an edge.
            if(m_id[i+m] > m_id[i])
                m_id[i+m]=m_id[i];
            else{
                m_id[m_id[i] - 1]=m_id[i+m];
                m_id[i]=m_id[i+m];
            }
        }
    }

    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The index of the point of interest.
     */
    private void lookUp(int i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(i % m != m-1 && i+1<m_id.length && m_id[i+1] > 0 ){
            ++m_edges; //there is an edge.
            if(m_id[i+1] > m_id[i])
                m_id[i+1] = m_id[i];
            else{
                m_id[m_id[i] - 1] = m_id[i+1];
                m_id[i] = m_id[i+1];
            }
        }
    }

	/**
	 * Find the root of point i.
	 * @param i  a point
	 */
	private int root(int i)
	{
		while (i != m_id[i])
		{
		//	m_id[i] = m_id[m_id[i]];
			i = m_id[i];
		}
		return i;
	}
	
	/**
	 * Update the roots of p and q, and
	 * return true if they are connected.
	 * @param p  a point.
	 * @param q  another point.
	 * @return   true if p and q are connected,
	 * 			 false if not.
	 */
	public boolean find(int p, int q)
	{
		return root(p) == root(q);
	}
	
	/**
	 * Join two trees. Choose the root which
	 * is the smallest as the common root.
	 * @param p  a point
	 * @param q  another point
	 */
	public void union(int p, int q)
	{
		int i = root(p);
		int j = root(q);
		if (i < j)
		{
			m_id[i] = j;
		}
		else
		{
			m_id[j] = i;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[3] = 0;
        vec[7] = 0;
        vec[11] = 0;
        vec[10] = 0;
        vec[9] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 1 5 0  13
         */
	}

}