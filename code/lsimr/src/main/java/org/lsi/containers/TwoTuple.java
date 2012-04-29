package org.lsi.containers;
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

	public T first(){
		return this.first;
	}

	public T second(){
		return this.second;
	}

}
