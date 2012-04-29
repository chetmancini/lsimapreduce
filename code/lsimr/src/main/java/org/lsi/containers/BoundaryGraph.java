package org.lsi.containers;
import java.util.HashMap;

public class BoundaryGraph{
    public HashMap<Integer, TwoTuple<Integer>> vertices = new HashMap<Integer, TwoTuple<Integer>>();
    public HashMap<Integer, Integer> sizes = new HashMap<Integer, Integer>(); 
    public Integer m;
    public Integer n;
    public Integer g;
}
