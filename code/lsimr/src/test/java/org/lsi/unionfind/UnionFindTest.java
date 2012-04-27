package org.lsi.unionfind;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.lsi.unionfind.UnionFind;
import static org.junit.Assert.*;
/**
 * Unit test for simple App.
 */
public class UnionFindTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public UnionFindTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( UnionFindTest.class );
    }

    /**
     * Rigourous Test :-)
     */
    public void testUnionFindOne()
    {
        
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[2] = 0;
        vec[6] = 0;
        vec[10] = 0;
        vec[9] = 0;
        vec[8] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 1 5 0  13
         */
        int[] expected = {1,1,0,4,1,1,0,4,0,0,0,4,4,4,4,4};

        assertArrayEquals(expected,uf.getRoots());
    }
    
    public void testUnionFindTwo()
    {
        
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[0] = 0;
        vec[2] = 0;
        vec[4] = 0;
        vec[6] = 0;
        vec[10] = 0;
        vec[9] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 0 0 9  13
         *
         */
        int[] expected = {0,2,0,4,0,2,0,4,4,0,0,4,4,4,4,4};

        assertArrayEquals(expected,uf.getRoots());
    }
    
    public void testUnionFindThree()
    {
        
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[0] = 0;
        vec[1] = 0;
        vec[2] = 0;
        vec[4] = 0;
        vec[6] = 0;
        vec[8] = 0;
        vec[10] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 0 6 10 14
         * 0 0 0  13
         *
         */
        int[] expected = {0,0,0,4,0,4,0,4,0,4,0,4,4,4,4,4};

        assertArrayEquals(expected,uf.getRoots());
    }

    public void testEdgesOne()
    {
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[2] = 0;
        vec[6] = 0;
        vec[10] = 0;
        vec[9] = 0;
        vec[8] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 1 5 0  13
         */

        assertEquals(10,uf.getEdges());
    }

    public void testEdgesTwo()
    {
        
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[0] = 0;
        vec[2] = 0;
        vec[4] = 0;
        vec[6] = 0;
        vec[10] = 0;
        vec[9] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 0 0 9  13
         *
         */

        assertEquals(8,uf.getEdges());
    }
    
    public void testEdgesThree(){
        int[] vec = new int[16];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 16; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        vec[0] = 0;
        vec[1] = 0;
        vec[2] = 0;
        vec[4] = 0;
        vec[6] = 0;
        vec[8] = 0;
        vec[10] = 0;
        
        UnionFind uf = new UnionFind(vec, 4, 4);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 0 6 10 14
         * 0 0 0  13
         *
         */
        assertEquals(8, uf.getEdges());
    }
 
}
