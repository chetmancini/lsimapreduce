package lsi;
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
	private int[] m_id;
	
	/**
	 * Data structure to efficiently keep track of
	 * connected components.
	 * @param n  the number of points in your space.
	 */
	public UnionFind(int[] id)
	{
		m_id = id;
	}
	
	/**
	 * Find the root of point i.
	 * @param i  a point
	 */
	private int root(int i)
	{
		while (i != m_id[i])
		{
			//TODO: Replace this ghetto shit 
			//		with a proper loop.
			m_id[i] = m_id[m_id[i]];
			i = m_id[i];
		}
		return i;
	}
	
	/**
	 * Update the roots of p and q, and
	 * return true if they are connected.
	 * @param p  a point.
	 * @param q  another point.
	 * @return   true if p and q are connected,
	 * 			 false if not.
	 */
	public boolean find(int p, int q)
	{
		return root(p) == root(q);
	}
	
	/**
	 * Join two trees. Choose the root which
	 * is the smallest as the common root.
	 * @param p  a point
	 * @param q  another point
	 */
	public void union(int p, int q)
	{
		int i = root(p);
		int j = root(q);
		if (i < j)
		{
			m_id[i] = j;
		}
		else
		{
			m_id[j] = i;
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
        /*
         * Make some test cases here.
         */
	}

}
