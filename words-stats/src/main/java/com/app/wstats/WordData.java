package com.app.wstats;

public class WordData implements Comparable<WordData> {
private String word;
private Integer frequency;
private Integer numVocals;
private Integer numConsonants;
private Character mostRepeatedChar;
private Integer mostRepeatedCharFreq;

public WordData(String w,Integer f) {
	this.word = w;
	this.frequency = f;
}

public String getWord() {
	return word;
}
public void setWord(String word) {
	this.word = word;
}
public Integer getFrequency() {
	return frequency;
}
public void setFrequency(Integer frequency) {
	this.frequency = frequency;
}

public Integer getNumVocals() {
	return numVocals;
}

public void setNumVocals(Integer numVocals) {
	this.numVocals = numVocals;
}

public Integer getNumConsonants() {
	return numConsonants;
}

public void setNumConsonants(Integer numConsonants) {
	this.numConsonants = numConsonants;
}

public Character getMostRepeatedChar() {
	return mostRepeatedChar;
}

public void setMostRepeatedChar(Character mostRepeatedChar) {
	this.mostRepeatedChar = mostRepeatedChar;
}



public Integer getMostRepeatedCharFreq() {
	return mostRepeatedCharFreq;
}

public void setMostRepeatedCharFreq(Integer mostRepeatedCharFreq) {
	this.mostRepeatedCharFreq = mostRepeatedCharFreq;
}

public int compareTo(WordData o) {
	// TODO Auto-generated method stub
	return frequency.compareTo(o.frequency);
}

@Override
public String toString() {
	// TODO Auto-generated method stub
	return this.word + ":" + this.frequency;
}






}
