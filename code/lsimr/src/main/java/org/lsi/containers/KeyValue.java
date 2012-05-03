package org.lsi.containers;

/**
 * @author Chet
 *
 * @param <K>
 * @param <V>
 */
public class KeyValue<K, V>{
	
	private K key;
	private V value;
	
	public KeyValue(K key, V value){
		this.key = key;
		this.value = value;
	}
	
	public void set(K key, V value){
		this.key = key;
		this.value = value;
	}
	
	public void put(V value){
		this.value = value;
	}
	
	public K getKey(){
		return this.key;
	}
	
	public V getValue(){
		return this.value;
	}
	
	/**
	 * ToString
	 */
	public String toString(){
		return this.key.toString() + "_" + this.value.toString();
	}
	
	
	
}
