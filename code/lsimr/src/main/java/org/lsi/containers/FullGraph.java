package org.lsi.containers;
import java.util.HashMap;
import org.lsi.containers.ComplexNumber;

public class FullGraph{
	
	/**
	 * Vertices
	 */
    public HashMap<ComplexNumber, ComplexNumber> vertices = new HashMap<ComplexNumber, ComplexNumber>();
    
    /**
     * Sizes
     */
    public HashMap<ComplexNumber, ComplexNumber> sizes = new HashMap<ComplexNumber, ComplexNumber>(); 
     
    //
    public Integer m;
    //
    public Integer n;
    //
    public Integer g;
}
