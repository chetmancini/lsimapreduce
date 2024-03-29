package org.lsi.unionfind;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.io.IntWritable;
import org.lsi.mapreduce.IntIntIntIntWritableTuple;
import org.lsi.mapreduce.IntIntWritableTuple;
import org.lsi.unionfind.UnionFind;
import org.lsi.containers.FullGraph;
import org.lsi.containers.ComplexNumber;
import static org.junit.Assert.*;

import java.util.ArrayList;
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
        HashMap<ComplexNumber,ComplexNumber> vec = fg.vertices;


        //using 1 based index so that 0 means no trees.
        for(Integer i = 0; i < 16; ++i)
            vec.put(new ComplexNumber(0,i), new ComplexNumber(0,i));
        
        //clear a path to disconnect a space.
        vec.remove(new ComplexNumber(0,2));
        vec.remove(new ComplexNumber(0,6));
        vec.remove(new ComplexNumber(0,10));
        vec.remove(new ComplexNumber(0,9));
        vec.remove(new ComplexNumber(0,8));

        System.out.println(vec);

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
        Integer[] result = uf.getTestOutput();  
        
        for(Integer i : result)
            System.out.println(i);
        assertArrayEquals(expected,uf.getTestOutput());
    }
//    
//    public void testUnionFindTwo()
//    {
//        
//        FullGraph fg = new FullGraph();
//        HashMap<Integer,Integer> vec = fg.vertices;
//        //using 1 based index so that 0 means no trees.
//        for(Integer i = 0; i < 16; ++i)
//            vec.put(i,i);
//        
//        //clear a path to disconnect a space.
//        vec.remove(0);
//        vec.remove(2);
//        vec.remove(4);
//        vec.remove(6);
//        vec.remove(10);
//        vec.remove(9);
//        
//        fg.m=4; fg.n=4; fg.g=4;
//        UnionFind uf = new UnionFind(fg);
//
//        /**
//         * Matrix that looks like:
//         * 4 8 12 16
//         * 0 0 0  15
//         * 2 6 0  14
//         * 0 0 9  13
//         *
//         */
//         Integer[] expected = {-1,1,-1,3,-1,1,-1,3,3,-1,-1,3,3,3,3,3};
//
//        assertArrayEquals(expected,uf.getTestOutput());
//    }
//    
//    public void testUnionFindThree()
//    {
//        
//        FullGraph fg = new FullGraph();
//        HashMap<Integer,Integer> vec = fg.vertices;
//        //using 1 based index so that 0 means no trees.
//        for(Integer i = 0; i < 16; ++i)
//            vec.put(i,i);
//        
//        //clear a path to disconnect a space.
//        vec.remove(0);
//        vec.remove(1);
//        vec.remove(2);
//        vec.remove(4);
//        vec.remove(6);
//        vec.remove(8);
//        vec.remove(10);
//        
//        fg.m=4; fg.n=4; fg.g=4;
//        UnionFind uf = new UnionFind(fg);
//
//        /**
//         * Matrix that looks like:
//         * 4 8 12 16
//         * 0 0 0  15
//         * 0 6 10 14
//         * 0 0 0  13
//         *
//         */
//        Integer[] expected = {-1,-1,-1,3,-1,3,-1,3,-1,3,-1,3,3,3,3,3};
//
//        assertArrayEquals(expected,uf.getTestOutput());
//    }
//    
    private void deleteFromToBy(Integer from, Integer to, Integer by, HashMap<ComplexNumber,ComplexNumber> vec){
        //adjust for zero based index.
        for(Integer i=from; i<=to; i+=by){
            vec.remove(new ComplexNumber(0,i));
        }
    }
    
    public void testUnionFindSnake()
    {
        
        FullGraph fg = new FullGraph();
        HashMap<ComplexNumber,ComplexNumber> vec = fg.vertices;
        for(Integer i = 0; i < 100; ++i)
            vec.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
        
        //clear a path to disconnect a space.
        deleteFromToBy(11,91,10, vec);
        deleteFromToBy(3,83,10,vec);
        vec.remove(new ComplexNumber(0,84));
        vec.remove(new ComplexNumber(0,65));
        vec.remove(new ComplexNumber(0,44));
        vec.remove(new ComplexNumber(0,25));
        vec.remove(new ComplexNumber(0,4));
        deleteFromToBy(16,96,10,vec);
        deleteFromToBy(8,88,10,vec);
        
        Integer[] expected = new Integer[100];
        for(Integer i=0; i<100; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                expected[i]=0;
            else
                expected[i]=-1;
        }
       
        FullGraph fg1 = new FullGraph();
        for(Integer i = 0; i < 40; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg1.vertices.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
        }
            
        FullGraph fg2 = new FullGraph();
        for(Integer i = 30; i < 70; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg2.vertices.put(new ComplexNumber(1,i-30),new ComplexNumber(1,i-30));
        }

        FullGraph fg3 = new FullGraph();
        for(Integer i = 60; i < 100; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg3.vertices.put(new ComplexNumber(2,i-60),new ComplexNumber(2,i-60));
        }

        fg1.m=10; fg1.n=4; fg1.g=10;
        fg2.m=10; fg2.n=4; fg2.g=10;
        fg3.m=10; fg3.n=4; fg3.g=10;
        UnionFind uf1 = new UnionFind(fg1);
        uf1.printRoots();
        UnionFind uf2 = new UnionFind(fg2);
        uf2.printRoots();
        UnionFind uf3 = new UnionFind(fg3);
        uf3.printRoots();

        
        HashMap<ComplexNumber, ComplexNumber> bc1 = new HashMap<ComplexNumber, ComplexNumber>();
        bc1 = uf1.getBoundaryColumns();
        
        HashMap<ComplexNumber, ComplexNumber> bc2 = new HashMap<ComplexNumber, ComplexNumber>();
        bc2 = uf2.getBoundaryColumns();

        HashMap<ComplexNumber, ComplexNumber> bc3 = new HashMap<ComplexNumber, ComplexNumber>();
        bc3 = uf3.getBoundaryColumns();

        HashMap<ComplexNumber, ComplexNumber> bc4 = new HashMap<ComplexNumber, ComplexNumber>();
        bc4.putAll(bc1);
        bc4.putAll(bc2);
        bc4.putAll(bc3);

        FullGraph fg4 = new FullGraph();
        fg4.vertices = bc4;
        fg4.m=10; fg4.n=4; fg4.g=10;
        
        UnionFind uf4 = new UnionFind(fg4);
        HashMap<ComplexNumber, ComplexNumber> vec3 = uf4.getRoots();
        System.out.println("================");
        uf4.printRoots();

        FullGraph fg13 = new FullGraph();
        for(Integer i = 0; i < 30; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg13.vertices.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
            if(vec3.containsKey(new ComplexNumber(0,i)))
                fg13.vertices.put(new ComplexNumber(0,i),vec3.get(new ComplexNumber(0,i)));
            
        }
            
        FullGraph fg23 = new FullGraph();
        for(Integer i = 30; i < 60; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg23.vertices.put(new ComplexNumber(1,i-30),new ComplexNumber(1,i-30));
            if(vec3.containsKey(new ComplexNumber(1,i)))
                fg23.vertices.put(new ComplexNumber(1,i),vec3.get(new ComplexNumber(1,i)));
        }

        FullGraph fg33 = new FullGraph();
        for(Integer i = 60; i < 100; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg33.vertices.put(new ComplexNumber(2,i-60),new ComplexNumber(2,i-60));
            if(vec3.containsKey(new ComplexNumber(2,i)))
                fg33.vertices.put(new ComplexNumber(2,i),vec3.get(new ComplexNumber(2,i)));
        }

        fg13.m=10; fg13.n=3; fg13.g=10;
        fg23.m=10; fg23.n=3; fg23.g=10;
        fg33.m=10; fg33.n=4; fg33.g=10;

        UnionFind uf13 = new UnionFind(fg13);
        UnionFind uf23 = new UnionFind(fg23);
        UnionFind uf33 = new UnionFind(fg33);

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
        assertArrayEquals(expected,expected);
    }
    public void testUnionFindBlock()
    {
        
        FullGraph fg = new FullGraph();
        HashMap<ComplexNumber,ComplexNumber> vec = fg.vertices;
        for(Integer i = 0; i < 100; ++i)
            vec.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
        
        
        Integer[] expected = new Integer[100];
        for(Integer i=0; i<100; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                expected[i]=0;
            else
                expected[i]=-1;
        }
       
        FullGraph fg1 = new FullGraph();
        for(Integer i = 0; i < 40; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg1.vertices.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
        }
            
        FullGraph fg2 = new FullGraph();
        for(Integer i = 30; i < 70; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg2.vertices.put(new ComplexNumber(1,i-30),new ComplexNumber(1,i-30));
        }

        FullGraph fg3 = new FullGraph();
        for(Integer i = 60; i < 100; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg3.vertices.put(new ComplexNumber(2,i-60),new ComplexNumber(2,i-60));
        }

        fg1.m=10; fg1.n=4; fg1.g=10;
        fg2.m=10; fg2.n=4; fg2.g=10;
        fg3.m=10; fg3.n=4; fg3.g=10;
        UnionFind uf1 = new UnionFind(fg1);
        uf1.printRoots();
        UnionFind uf2 = new UnionFind(fg2);
        uf2.printRoots();
        UnionFind uf3 = new UnionFind(fg3);
        uf3.printRoots();

        
        HashMap<ComplexNumber, ComplexNumber> bc1 = new HashMap<ComplexNumber, ComplexNumber>();
        bc1 = uf1.getBoundaryColumns();
        
        HashMap<ComplexNumber, ComplexNumber> bc2 = new HashMap<ComplexNumber, ComplexNumber>();
        bc2 = uf2.getBoundaryColumns();

        HashMap<ComplexNumber, ComplexNumber> bc3 = new HashMap<ComplexNumber, ComplexNumber>();
        bc3 = uf3.getBoundaryColumns();

        HashMap<ComplexNumber, ComplexNumber> bc4 = new HashMap<ComplexNumber, ComplexNumber>();
        bc4.putAll(bc1);
        bc4.putAll(bc2);
        bc4.putAll(bc3);

        FullGraph fg4 = new FullGraph();
        fg4.vertices = bc4;
        fg4.m=10; fg4.n=4; fg4.g=10;
        
        UnionFind uf4 = new UnionFind(fg4);
        HashMap<ComplexNumber, ComplexNumber> vec3 = uf4.getRoots();
        System.out.println("================");
        uf4.printRoots();

        FullGraph fg13 = new FullGraph();
        for(Integer i = 0; i < 30; ++i){
            if(vec.containsKey(new ComplexNumber(0,i)))
                fg13.vertices.put(new ComplexNumber(0,i),new ComplexNumber(0,i));
        }
            
        FullGraph fg23 = new FullGraph();
        for(Integer i = 30; i < 60; ++i){
            if(vec3.containsKey(new ComplexNumber(0,i)))
                fg23.vertices.put(new ComplexNumber(1,i-30),vec3.get(new ComplexNumber(0,i)));
            else if(vec.containsKey(new ComplexNumber(0,i)))
                fg23.vertices.put(new ComplexNumber(1,i-30),new ComplexNumber(1,i-30));
        }

        FullGraph fg33 = new FullGraph();
        for(Integer i = 60; i < 100; ++i){
            if(vec3.containsKey(new ComplexNumber(1,i-30)))
                fg33.vertices.put(new ComplexNumber(2,i-60),vec3.get(new ComplexNumber(1,i-30)));
            else if(vec.containsKey(new ComplexNumber(0,i)))
                fg33.vertices.put(new ComplexNumber(2,i-60),new ComplexNumber(2,i-60));
        }

        System.out.println(fg33.vertices);

        fg13.m=10; fg13.n=3; fg13.g=10;
        fg23.m=10; fg23.n=3; fg23.g=10;
        fg33.m=10; fg33.n=4; fg33.g=10;

        UnionFind uf13 = new UnionFind(fg13);
        UnionFind uf23 = new UnionFind(fg23);
        UnionFind uf33 = new UnionFind(fg33);

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

        uf13.printRoots();
        uf23.printRoots();
        uf33.printRoots();
        
	    HashMap<ComplexNumber, Integer> sizesForRoot1 = uf13.getSizes();
	    HashMap<ComplexNumber, Integer> sizesForRoot2 = uf23.getSizes();
	    HashMap<ComplexNumber, Integer> sizesForRoot3 = uf33.getSizes();
        System.out.println(sizesForRoot1);
        System.out.println(sizesForRoot2);
        System.out.println(sizesForRoot3);
			
        //for(ComplexNumber c : sizesForRoot.keySet()) {
        //    parentGlobalId.set(MrProj.getGlobalFromIdInColumnGroup(c.index, c.groupid, 4, 100));
        //    sizeComponent.set(sizesForRoot.get(c));
        //    output.collect(parentGlobalId,sizeComponent);
        //}
        
        assertArrayEquals(expected,expected);
    } 
//    public void testEdgesOne()
//    {
//        FullGraph fg = new FullGraph();
//        HashMap<Integer,Integer> vec = fg.vertices;
//        //using 1 based index so that 0 means no trees.
//        for(Integer i = 0; i < 16; ++i)
//            vec.put(i,i);
//        
//        //clear a path to disconnect a space.
//        vec.remove(2);
//        vec.remove(6);
//        vec.remove(10);
//        vec.remove(9);
//        vec.remove(8);
//        
//        fg.m=4; fg.n=4; fg.g=4;
//        UnionFind uf = new UnionFind(fg);
//
//        /**
//         * Matrix that looks like:
//         * 4 8 12 16
//         * 0 0 0  15
//         * 2 6 0  14
//         * 1 5 0  13
//         */
//
//        assertEquals(10,(int)uf.getEdges());
//    }
//
//    public void testEdgesTwo()
//    {
//        FullGraph fg = new FullGraph();
//        HashMap<Integer,Integer> vec = fg.vertices;
//        //using 1 based index so that 0 means no trees.
//        for(Integer i = 0; i < 16; ++i)
//            vec.put(i,i);
//        
//        //clear a path to disconnect a space.
//        vec.remove(0);
//        vec.remove(2);
//        vec.remove(4);
//        vec.remove(6);
//        vec.remove(10);
//        vec.remove(9);
//        
//        fg.m=4; fg.n=4; fg.g=4;
//        UnionFind uf = new UnionFind(fg);
//
//        /**
//         * Matrix that looks like:
//         * 4 8 12 16
//         * 0 0 0  15
//         * 2 6 0  14
//         * 0 0 9  13
//         *
//         */
//
//        assertEquals(8,(int)uf.getEdges());
//    }
//    
//    public void testEdgesThree(){
//        FullGraph fg = new FullGraph();
//        HashMap<Integer,Integer> vec = fg.vertices;
//        //using 1 based index so that 0 means no trees.
//        for(Integer i = 0; i < 16; ++i)
//            vec.put(i,i);
//        
//        //clear a path to disconnect a space.
//        vec.remove(0);
//        vec.remove(1);
//        vec.remove(2);
//        vec.remove(4);
//        vec.remove(6);
//        vec.remove(8);
//        vec.remove(10);
//        
//        fg.m=4; fg.n=4; fg.g=4;
//        UnionFind uf = new UnionFind(fg);
//
//        /**
//         * Matrix that looks like:
//         * 4 8 12 16
//         * 0 0 0  15
//         * 0 6 10 14
//         * 0 0 0  13
//         *
//         */
//        assertEquals(8, (int)uf.getEdges());
//    }
    
    public void testEdges(){
    	ArrayList<IntIntWritableTuple> list0 = new ArrayList<IntIntWritableTuple>();
    	ArrayList<IntIntWritableTuple> list1 = new ArrayList<IntIntWritableTuple>();

    	for(int i = 0; i<2; i++) {
    		IntIntWritableTuple cellAndParent0 = new IntIntWritableTuple();
    		cellAndParent0.set(10*i+0, 10*i+0);
    		IntIntWritableTuple cellAndParent1 = new IntIntWritableTuple();
    		cellAndParent1.set(10*i+1, 10*i+1);
    		IntIntWritableTuple cellAndParent2 = new IntIntWritableTuple();
    		cellAndParent2.set(10*i+2, 10*i+2);
    		IntIntWritableTuple cellAndParent3 = new IntIntWritableTuple();
    		cellAndParent3.set(10*i+3, 10*i+3);
    		IntIntWritableTuple cellAndParent4 = new IntIntWritableTuple();
    		cellAndParent4.set(10*i+4, 10*i+4);
    		
    		list0.add(cellAndParent0);
    		list0.add(cellAndParent1);
    		list0.add(cellAndParent2);
    		list0.add(cellAndParent3);
    		list0.add(cellAndParent4);
    	}
    	
    	for(int i = 0; i<2; i++) {
    		IntIntWritableTuple cellAndParent0 = new IntIntWritableTuple();
    		cellAndParent0.set(10*i+10, 10*i+10);
    		IntIntWritableTuple cellAndParent1 = new IntIntWritableTuple();
    		cellAndParent1.set(10*i+11, 10*i+11);
    		IntIntWritableTuple cellAndParent2 = new IntIntWritableTuple();
    		cellAndParent2.set(10*i+12, 10*i+12);
    		IntIntWritableTuple cellAndParent3 = new IntIntWritableTuple();
    		cellAndParent3.set(10*i+13, 10*i+13);
    		IntIntWritableTuple cellAndParent4 = new IntIntWritableTuple();
    		cellAndParent4.set(10*i+14, 10*i+14);
    		
    		list1.add(cellAndParent0);
    		list1.add(cellAndParent1);
    		list1.add(cellAndParent2);
    		list1.add(cellAndParent3);
    		list1.add(cellAndParent4);
    	}
    	
    	/****
    	 * 1 0 1 0 1
    	 * 1 0 1 0 1
    	 * 1 0 1 0 1
    	 * 1 0 1 0 1
    	 * 1 0 1 0 1
    	 * 
    	 * With 2 column gps of dim 3
    	 */
    	
    	System.out.println("\n\n\n\n\n");
    	
    	UnionFind uf0 = new UnionFind(new IntWritable(0),list0.iterator(),5,3,false);
    	System.out.println("\n\n\n\n\n");
    	UnionFind uf1 = new UnionFind(new IntWritable(1),list1.iterator(),5,3,false);
    	
    	System.out.println("uf0 is "+uf0.getEdges());
    	System.out.println("uf1 is "+uf1.getEdges());
    	
    	assertEquals(12, uf0.getEdges()+uf1.getEdges());
    	

    	uf0 = new UnionFind(new IntWritable(0),list0.iterator(),5,3,true);
    	System.out.println("\n\n\n\n\n");
    	uf1 = new UnionFind(new IntWritable(1),list1.iterator(),5,3,true);
    	
    	System.out.println("uf0 is "+uf0.getEdges());
    	System.out.println("uf1 is "+uf1.getEdges());
    	
    	assertEquals(12, uf0.getEdges()+uf1.getEdges());
    }
}
