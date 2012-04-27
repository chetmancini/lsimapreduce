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
    
    private void deleteFromToBy(int from, int to, int by, int[] vec){
        //adjust for zero based index.
        from=from-1;
        to=to;
        for(int i=from; i<to; i+=by){
            vec[i]=0;
        }
    }
    
    public void testUnionFindSnake()
    {
        
        int[] vec = new int[100];
        //using 1 based index so that 0 means no trees.
        for(int i = 0; i < 100; ++i)
            vec[i]=i+1;
        
        //clear a path to disconnect a space.
        deleteFromToBy(12,92,10, vec);
        deleteFromToBy(4,84,10,vec);
        vec[84]=0;
        vec[65]=0;
        vec[44]=0;
        vec[25]=0;
        vec[4]=0;
        deleteFromToBy(17,97,10,vec);
        deleteFromToBy(9,89,10,vec);
        
        int[] expected = new int[100];
        for(int i=0; i<100; ++i){
            if(vec[i]==0)
                expected[i]=0;
            else
                expected[i]=1;
        }

        UnionFind uf = new UnionFind(vec, 10, 10);

        /**
         * Matrix that looks like:
         * 10 20 30 40 50 60 70 80 90 100
         *                             99
         * 8  18 28 38 48 58 68 78 88  98
         * 7  
         * 6  16    36 46 56    76 86  96 
         *    15 25 35    55 65 75     95
         *                             94
         * 3  13 23 33 43 53 63 73 83  93 
         * 2  
         * 1  11 21 31 41 51 61 71 81  91 
         *
         */
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
