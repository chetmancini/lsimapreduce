package org.lsi.unionfind;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.lsi.containers.ComplexNumber;
import org.lsi.containers.FullGraph;
import org.lsi.mapreduce.*; 
import org.lsi.mapreduce.IntIntWritableTuple;
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
    private HashMap<ComplexNumber, ComplexNumber> m_id = new HashMap<ComplexNumber, ComplexNumber>();
    private Integer m = 0;
    private Integer n = 0;
    private Integer m_g = 0;
    private Integer m_edges = 0;
    private HashMap<ComplexNumber, Integer> m_sizes = new HashMap<ComplexNumber, Integer>();

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

        run();
    }

    private void run(){
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

        List<ComplexNumber> keys = new ArrayList<ComplexNumber>(m_id.keySet());
        Collections.sort(keys);
        for(Integer j=0; j<2; ++j)
        {
            for(ComplexNumber i : keys) 
            {
                //System.out.println(i);
                lookLeft(i);
                lookDown(i);
            }
        }

        //We double count every vertex when we make two
        //passes, so have to devide by two here.
        m_edges = m_edges/2;
    }


    public UnionFind(IntWritable groupid, Iterator<IntIntWritableTuple> idsCells, Integer m, Integer n)
    {
        this.m = m;
        this.n = n;
        while(idsCells.hasNext()){
            IntIntWritableTuple c = idsCells.next();
            ComplexNumber k = new ComplexNumber(groupid.get(), c.i);
            ComplexNumber v = new ComplexNumber(groupid.get(), c.parent);
            m_id.put(k,v);
        }            
        run();
    }

    public UnionFind(Iterator<IntIntIntWritableTuple> idsCells, Integer m, Integer n)
    {
        this.m = m;
        this.n = n;
        while(idsCells.hasNext()){
            IntIntIntWritableTuple c = idsCells.next();
            ComplexNumber k = new ComplexNumber(c.groupid,c.i);
            ComplexNumber v = new ComplexNumber(c.groupid,c.parent); 
            m_id.put(k,v);
        }
    }

    /**
     * Determines if a point is in a boundary column.
     * @param the one based index of the point of interest.
     */
    boolean isBoundary(Integer i)
    {
        return ( (i/m)%(m_g-1) == 0 );
    }

    public HashMap<ComplexNumber, ComplexNumber> getBoundaryColumns()
    {
        HashMap<ComplexNumber, ComplexNumber> bc = new HashMap<ComplexNumber, ComplexNumber>();
        Iterator it = m_id.entrySet().iterator();
        while(it.hasNext())
        {
            Map.Entry pairs = (Map.Entry)it.next(); 
            ComplexNumber k = (ComplexNumber)pairs.getKey();
            ComplexNumber v = (ComplexNumber)pairs.getValue();
            bc.put(k,v); 
        }
        return bc;
    }

    public HashMap<ComplexNumber, ComplexNumber> getRoots()
    {
        return m_id;
    }

    public void printRoots()
    {
        Iterator it = m_id.entrySet().iterator();
        while(it.hasNext())
        {
            Map.Entry pairs = (Map.Entry)it.next(); 
            ComplexNumber k = (ComplexNumber)pairs.getKey();
            ComplexNumber v = (ComplexNumber)pairs.getValue();
            System.out.println("{" + k + "," + v + "}"); 
        }
    }

    public Integer getRoot(Integer index) throws Exception
    {
        throw new Exception("getRoot takes a ComplexNumber, not Integer");
    }

    public ComplexNumber getRoot(Integer groupid, Integer index)
    {
        ComplexNumber c = new ComplexNumber(groupid, index);
        return m_id.get(c);
    }

    public Integer[] getTestOutput()
    {
        Integer[] out = new Integer[m*n];
        for(Integer i = 0; i < m*n; ++i){
            ComplexNumber k = new ComplexNumber(0,i);
            if (m_id.containsKey(k))
                out[i]=m_id.get(k).index;
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

    /*
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookLeft(ComplexNumber i)
    {
        //if spot above me is a tree, and i'm not in the
        //top row.
        ComplexNumber left;
        if(i.index < m)
            //column group to the left, right boundary column
            left = new ComplexNumber(i.groupid-1, m*n - m + i.index);
        else
            //same column group, just look one column left.
            left = new ComplexNumber(i.groupid, i.index-m);
        if(m_id.containsKey(i) && m_id.containsKey(left)){
            ++m_edges; //there is an edge.
            unite(i, left);  //converted to one based.
        }
    }

    
    /**
     * Return the id of the element below, if there
     * is a tree there, otherwise return own id.
     * @param i The zero based index of the point of interest.
     */
    private void lookDown(ComplexNumber i)
    {
        ComplexNumber down = new ComplexNumber(i.groupid, i.index-1);
        //if spot above me is a tree, and i'm not in the
        //top row.
        if(m_id.containsKey(i) && m_id.containsKey(down) && row(i) != 0){
            ++m_edges; //there is an edge.
            unite(i, down);
        }
    }


    /**
     * Unite p and q
     * @param p the zero based index of a point
     * @param q the zero based index of a point
     */
    private void unite(ComplexNumber p, ComplexNumber q)
    {
        //System.out.println("Uniting " + p + ", " + q);
        ComplexNumber i = root(p);
        ComplexNumber j = root(q);
        //System.out.println("With roots " + i + ", " + j);
        if(i.lessThan(j))
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
    private ComplexNumber root(ComplexNumber i)
    {
        while(m_id.containsKey(i) && i != m_id.get(i)){
            while(m_id.containsKey(i) && m_id.get(i) != m_id.get(m_id.get(i)))
                m_id.put(i,m_id.get(m_id.get(i)));
            i = m_id.get(i);
        }
        return i;   
    }

    private Integer row(ComplexNumber i)
    {
        return i.index % m;
    }
    
    /**
     * STUB
     * TODO: IMPLEMENT
     * @param tup
     * @return
     */
    public int getSizeCCInColumn(int parent){
    	return 0;
    }
    
    public HashMap<ComplexNumber,ComplexNumber> getRootsHashMap() {
    	return this.m_id;
    }
    
}
