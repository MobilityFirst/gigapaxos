package edu.umass.cs.utils;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author arun
 *
 * The purpose of this class is to wrap different objects into 
 * this class so that the toString method can later give us a 
 * string if needed. This optimizes logging because the logger
 * won't actually call the toString method unless the log level
 * actually demands it.
 */
public class Stringer {
	final Object data;
	final int offset;
	final int length;

	/**
	 * @param data 
	 */
	public Stringer(byte[] data) {
		this.data = data;
		this.offset=0;
		this.length=data.length;
	}
	/**
	 * @param data
	 * @param offset
	 * @param length
	 */
	public Stringer(byte[] data, int offset, int length) {
		this.data = data;
		this.offset = offset;
		this.length = length;
	}

	/**
	 * @param data
	 */
	public Stringer(Object data) {
		this.data = data;
		this.offset=0;
		this.length=0;
	}

	public String toString() {
		if(data instanceof byte[])
			return new String((byte[])data, offset, length);
		else if(data instanceof Integer[]) 
			return (new HashSet<Integer>(Arrays.asList((Integer[])data))).toString();
		return data.toString();
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		byte[] buf = "hello world".getBytes();
		System.out.println(new Stringer(buf));
		Integer[] intArray = {23, 43, 56};
		System.out.println(new Stringer(intArray));
	}
}