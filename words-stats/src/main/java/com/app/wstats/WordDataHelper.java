package com.app.wstats;

public class WordDataHelper {
private WordData wordData = null;
private static char[] vocals = {'a','e','i','o','u'};
private static char[] consonants = {'b','c','d','f','g','h','j','k','l','m','n','o','p','q','r','s','t','v','w','x','y','z'};

public void setWordData(WordData wd) {
		this.wordData = wd;
	}

	public WordData compute() throws Exception {
		Integer nv,nc;
		Character mrc;
		nv = Integer.valueOf(computeVocals());
		nc = computeConsonants(nv);
		mrc = computeMRepeatedChar();
	
		this.wordData.setNumVocals(nv);
		this.wordData.setNumConsonants(nc);
		this.wordData.setMostRepeatedChar(mrc);
		return this.wordData;
	}

	public int computeVocals() throws Exception {
		char[] wc = wordData.getWord().toCharArray();
		int nv = 0;
		for(int i=0;i<wc.length;i++) {
			for(int j=0;j<vocals.length;j++) {
				if(wc[i]==vocals[j])
					nv++;
			}
		}
		return nv;
	}

	public int computeConsonants(int nvocals) throws Exception {
		int nc = this.wordData.getWord().length() - nvocals;
		return nc;
	}


	public char computeMRepeatedChar() throws Exception {
		char[] wc = wordData.getWord().toCharArray();
		char c = '@';
		int max = 1;
		int n = 0;
		for(int i=0;i<wc.length-1;i++) {
			n = 1;
			for(int j=i+1;j<wc.length;j++) {
				if(wc[i]==wc[j]) {
					n++;
					
				}
			}
			if(n>max) {
				max = n;
				c = wc[i];
			}
		}
		this.wordData.setMostRepeatedCharFreq(max);
		return c;
	}


}
