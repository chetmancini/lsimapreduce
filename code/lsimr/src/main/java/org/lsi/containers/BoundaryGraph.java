package lsimr.src.main.java.org.lsi.containers;
import java.util.HashMap;

public class BoundaryGraph{
	
	/**
	 * Vertices
	 */
    public HashMap<Integer, IntegerPair> vertices = new HashMap<Integer, IntegerPair>();
    
    /**
     * Sizes
     */
    public HashMap<Integer, Integer> sizes = new HashMap<Integer, Integer>(); 
    
    //
    public Integer m;
    //
    public Integer n;
    //
    public Integer g;
}
