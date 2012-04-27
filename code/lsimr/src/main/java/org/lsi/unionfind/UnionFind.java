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
        //
        //Also, we make two passes.  First pass, we initialize
        //the parents of every vertex by inspecting the left and
        //down vertices from them and seeing if either is a lower
        //index.  If they are different, unite them and choose
        //the lower root.
        //
        //While uniting, we run up the tree and replace the roots
        //all the way up.  Note that we only replace the roots, not
        //the vertices that might also be children of that root we
        //replaced.
        //
        //The second pass, it does exactly the same, but now the
        //the roots (which are scanned first since we go from smallest
        //to highest), will all be of the minimum index, so we use the
        //second pass to make sure all children are filled in consistently.
        for(int j=0; j<2; ++j){
            for(int i=0; i<m_id.length; ++i)
            {
                if(m_id[i] > 0)
                {
                    lookLeft(i);
                    lookDown(i);
                }
                
            }
        }

        //We double count every vertex when we make two
        //passes, so have to devide by two here.
        m_edges = m_edges/2;
    }

    public int[] getRoots(){
        return m_id;
    }

    public int getEdges(){
        return m_edges;
    }

    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookLeft(int i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(isTree(i-m)){
            ++m_edges; //there is an edge.
            unite(i+1, i+1-m);  //converted to one based.
        }
    }

    
    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookDown(int i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(isTree(i-1) && row(i) != 0){
            ++m_edges; //there is an edge.
            unite(i+1, i);  //converted to one based.
        }
    }


    /**
     * Unite p and q
     * @param p the one based index of a point
     * @param q the one based index of a point
     */
    private void unite(int p, int q)
    {
        System.out.println("Uniting " + p + ", " + q);
        int i = root(p);
        int j = root(q);
        System.out.println("With roots " + i + ", " + j);
        if(i < j)
            m_id[j-1] = i;
        else
            m_id[i-1] = j;
    }

    /**
     * Return the root of i.
     * @param i the one based index of the point.
     * @return the one based id of of the root of the tree
     *         containing point i.
     */ 
    private int root(int i)
    {
        while(i != m_id[i-1]){
            while(m_id[i-1] != m_id[m_id[i-1]-1])
                m_id[i-1] = m_id[m_id[i-1]-1];
            i = m_id[i-1];
        }
        return i;   
    }

    /**
     * Conveniently detect if there is a vertex at i, and if so
     * return true.  If i is out of bounds, or there is no vertex,
     * return false.
     * @param i The index of the point in question.
     * @return true if there is a vertex, false otherwise.
     */
    private boolean isTree(int i)
    {
        if(i >= 0 && i < m_id.length && m_id[i] > 0)
            return true;
        return false;
    }
    
    private int row(int i)
    {
        return i % m;
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
}
