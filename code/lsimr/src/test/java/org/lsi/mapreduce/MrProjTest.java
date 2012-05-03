package org.lsi.mapreduce;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.lsi.unionfind.UnionFind;
import org.lsi.containers.FullGraph;
import org.lsi.containers.ComplexNumber;
import static org.junit.Assert.*;
import java.util.HashMap;
/**
 * Unit test for simple App.
 */
public class MrProjTest
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public MrProjTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( MrProjTest.class );
    }

    
    public void testMrProjGetColumnGroupNbrsFromLine()
    {
    	int matrixSize = 40;
    	int columnWidth = 4;

    	int nbrColumnGroupExpected = (matrixSize - 1)/(columnWidth - 1) + ((matrixSize - 1)%(columnWidth - 1)==0?0:1);
    	int nbrCellPerColumnGroupExpected = columnWidth * matrixSize;
    	
    	HashMap<Integer, Integer> nbrCellPerColumnGroup = new HashMap<Integer,Integer>();
    	
    	
        for(int i = 0; i< matrixSize*matrixSize ; i++) {
        	int id = MrProj.getIdFromLine(i, matrixSize);
        	int[] columnId = MrProj.getColumnGroupNbrsFromLine(i, columnWidth, matrixSize);
        	for(int j : columnId) {
        		if(nbrCellPerColumnGroup.get(j) == null)
        			nbrCellPerColumnGroup.put(j,0);
             	nbrCellPerColumnGroup.put(j, nbrCellPerColumnGroup.get(j)+1);
            	System.out.println("Column "+MrProj.getIndices(i).getJ()+" in group "+j);
             	
             	assertTrue( MrProj.getIdInColumnGroupFromLine(i, j, columnWidth, matrixSize) >= 0);
             	assertTrue( MrProj.getIdInColumnGroupFromLine(i, j, columnWidth, matrixSize) < columnWidth*matrixSize);
             	
             	assertTrue( MrProj.getIdInColumnGroupFromId(id, j, columnWidth, matrixSize) >= 0);
             	assertTrue( MrProj.getIdInColumnGroupFromId(id, j, columnWidth, matrixSize) < columnWidth*matrixSize);     	
        	}
        }
        
        for(int i : nbrCellPerColumnGroup.keySet())
        	System.out.println(i + " : " + nbrCellPerColumnGroup.get(i));
        
        assertEquals(nbrColumnGroupExpected, nbrCellPerColumnGroup.keySet().size());
        
        for(int i=0; i<nbrColumnGroupExpected; i++) {
        	//Last column group ends perfectly at the end of the matrix
        	if((matrixSize -1)%(columnWidth -1) == 0)
        		assertEquals(new Integer(nbrCellPerColumnGroupExpected), nbrCellPerColumnGroup.get(i));
        	else {
        		if(i != nbrColumnGroupExpected -1 )
            		assertEquals(new Integer(nbrCellPerColumnGroupExpected), nbrCellPerColumnGroup.get(i));
        		else {
            		int nbrResidualColumns = (matrixSize -1)%(columnWidth -1);
            		int nbrCellPerResidualColumnGroupExpected = (1 + nbrResidualColumns)*matrixSize;
            		assertEquals(new Integer(nbrCellPerResidualColumnGroupExpected), nbrCellPerColumnGroup.get(i));
        		}
        	}
        }
    }
}
