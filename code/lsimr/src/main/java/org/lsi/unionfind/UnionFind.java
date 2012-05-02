package org.lsi.unionfind;
import java.util.HashMap;
import org.lsi.containers.FullGraph;
import org.lsi.mapreduce.*; 
import java.util.Iterator;
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
    private HashMap<Integer, Integer> m_id;
    private Integer m = 0;
    private Integer n = 0;
    private Integer m_g = 0;
    private Integer m_edges = 0;
    private HashMap<Integer, Integer> m_sizes;

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
    public UnionFind(FullGraph id)
    {
        this.m_id = id.vertices;
        this.m = id.m;
        this.n = id.n;
        this.m_g = id.g;
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
        for(Integer j=0; j<2; ++j){
            for(Integer i=0; i<m*m_g; ++i)
            {
                if(m_id.containsKey(i))
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


    public UnionFind(Iterator<IntIntWritableTuple> idsCells)
    {
        //XXX: DOOITTTT
    }

    /**
     * Determines if a point is in a boundary column.
     * @param the one based index of the point of interest.
     */
    boolean isBoundary(Integer i){
        return ( (i/m)%(m_g-1) == 0 );
    }

    public HashMap<Integer, Integer> getBoundaryCols()
    {
        HashMap<Integer,Integer> b = new HashMap<Integer, Integer>();
        for(Integer i = 0; i < m*m_g; ++i)
            if(isBoundary(i))
                b.put(i, m_id.get(i));
        return b;
    }

    public HashMap<Integer, Integer> getRoots(){
        return m_id;
    }

    public Integer getRoot(Integer i){
        return m_id.get(i);
    }

    public Integer[] getTestOutput(){
        Integer[] out = new Integer[m*m_g];
        for(Integer i = 0; i < m*m_g; ++i){
            if (m_id.containsKey(i))
                out[i]=m_id.get(i);
            else
                out[i]=-1;
        }
        return out;
    }        


    public Integer getEdges(){
        return m_edges;
    }

    public HashMap<Integer, Integer> getUniqueRoots(){
        //XXX:Implement. Probably change to set.
        return null;
    }
    
    public Integer getComponentCount(){
        //XXX:Implement.
        return 0;
    }

    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookLeft(Integer i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(m_id.containsKey(i) && m_id.containsKey(i-m)){
            ++m_edges; //there is an edge.
            unite(i, i-m);  //converted to one based.
        }
    }

    
    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookDown(Integer i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(m_id.containsKey(i) && m_id.containsKey(i-1) && row(i) != 0){
            ++m_edges; //there is an edge.
            unite(i, i-1);
        }
    }


    /**
     * Unite p and q
     * @param p the zero based index of a point
     * @param q the zero based index of a point
     */
    private void unite(Integer p, Integer q)
    {
        System.out.println("Uniting " + p + ", " + q);
        Integer i = root(p);
        Integer j = root(q);
        System.out.println("With roots " + i + ", " + j);
        if(i < j)
            m_id.put(j,i);
        else
            m_id.put(i,j);
    }

    /**
     * Return the root of i.
     * @param i the zero based index of the point.
     * @return the zero based id of of the root of the tree
     *         containing point i.
     */ 
    private Integer root(Integer i)
    {
        while(m_id.containsKey(i) && i != m_id.get(i)){
            while(m_id.containsKey(i) && m_id.get(i) != m_id.get(m_id.get(i)))
                m_id.put(i,m_id.get(m_id.get(i)));
            i = m_id.get(i);
        }
        return i;   
    }

    private Integer row(Integer i)
    {
        return i % m;
    }
    
    /**
     * STUB
     * TODO: IMPLEMENT
     * @param tup
     * @return
     */
    public int getNbrSizeInThisColumn(int parent){
    	return 0;
    }
    
}
