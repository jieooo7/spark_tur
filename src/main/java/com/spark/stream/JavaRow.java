package com.spark.stream;

/**
 * Created by thy on 17-6-20.
 */
public class JavaRow implements java.io.Serializable {
	private String word;
	
	public String getWord() {
		return word;
	}
	
	public void setWord(String word) {
		this.word = word;
	}
}
