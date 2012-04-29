package org.lsi.unionfind;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.lsi.unionfind.UnionFind;
import org.lsi.containers.TwoTuple;
import org.lsi.containers.FullGraph;
import org.lsi.containers.BoundaryGraph;
import static org.junit.Assert.*;
import java.util.HashMap;
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
        
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(2);
        vec.remove(6);
        vec.remove(10);
        vec.remove(9);
        vec.remove(8);

        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 1 5 0  13
         */
       
        Integer[] expected = {0,0,-1,3,0,0,-1,3,-1,-1,-1,3,3,3,3,3};
        

        assertArrayEquals(expected,uf.getTestOutput());
    }
    
    public void testUnionFindTwo()
    {
        
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(0);
        vec.remove(2);
        vec.remove(4);
        vec.remove(6);
        vec.remove(10);
        vec.remove(9);
        
        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 0 0 9  13
         *
         */
         Integer[] expected = {-1,1,-1,3,-1,1,-1,3,3,-1,-1,3,3,3,3,3};

        assertArrayEquals(expected,uf.getTestOutput());
    }
    
    public void testUnionFindThree()
    {
        
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(0);
        vec.remove(1);
        vec.remove(2);
        vec.remove(4);
        vec.remove(6);
        vec.remove(8);
        vec.remove(10);
        
        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 0 6 10 14
         * 0 0 0  13
         *
         */
        Integer[] expected = {-1,-1,-1,3,-1,3,-1,3,-1,3,-1,3,3,3,3,3};

        assertArrayEquals(expected,uf.getTestOutput());
    }
    
    private void deleteFromToBy(Integer from, Integer to, Integer by, HashMap<Integer,Integer> vec){
        //adjust for zero based index.
        for(Integer i=from; i<=to; i+=by){
            vec.remove(i);
        }
    }
    
    public void testUnionFindSnake()
    {
        
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        for(Integer i = 0; i < 100; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        deleteFromToBy(11,91,10, vec);
        deleteFromToBy(3,83,10,vec);
        vec.remove(84);
        vec.remove(65);
        vec.remove(44);
        vec.remove(25);
        vec.remove(4);
        deleteFromToBy(16,96,10,vec);
        deleteFromToBy(8,88,10,vec);
        
        Integer[] expected = new Integer[100];
        for(Integer i=0; i<100; ++i){
            if(vec.containsKey(i))
                expected[i]=0;
            else
                expected[i]=-1;
        }

        fg.m=10; fg.n=10; fg.g=10;
        UnionFind uf = new UnionFind(fg);

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
        assertArrayEquals(expected,uf.getTestOutput());
    }

    public void testEdgesOne()
    {
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(2);
        vec.remove(6);
        vec.remove(10);
        vec.remove(9);
        vec.remove(8);
        
        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 1 5 0  13
         */

        assertEquals(10,(int)uf.getEdges());
    }

    public void testEdgesTwo()
    {
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(0);
        vec.remove(2);
        vec.remove(4);
        vec.remove(6);
        vec.remove(10);
        vec.remove(9);
        
        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 2 6 0  14
         * 0 0 9  13
         *
         */

        assertEquals(8,(int)uf.getEdges());
    }
    
    public void testEdgesThree(){
        FullGraph fg = new FullGraph();
        HashMap<Integer,Integer> vec = fg.vertices;
        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(i,i);
        
        //clear a path to disconnect a space.
        vec.remove(0);
        vec.remove(1);
        vec.remove(2);
        vec.remove(4);
        vec.remove(6);
        vec.remove(8);
        vec.remove(10);
        
        fg.m=4; fg.n=4; fg.g=4;
        UnionFind uf = new UnionFind(fg);

        /**
         * Matrix that looks like:
         * 4 8 12 16
         * 0 0 0  15
         * 0 6 10 14
         * 0 0 0  13
         *
         */
        assertEquals(8, (int)uf.getEdges());
    }
 
}
