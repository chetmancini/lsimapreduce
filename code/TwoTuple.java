
/**
 * @author chet
 * @param <T>
 */
public class TwoTuple<T>{
	
	private T first;
	private T second;

	/**
	 * TwoTuple
	 */
	public TwoTuple(T first, T second){
		this.first = first;
		this.second = second;
	}

	/**
	 * Get first
	 * @return
	 */
	public T first(){
		return this.first;
	}

	/**
	 * Get second
	 * @return
	 */
	public T second(){
		return this.second;
	}

}