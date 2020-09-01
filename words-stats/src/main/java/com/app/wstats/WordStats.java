package com.app.wstats;

	
	import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
	import java.util.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.google.common.collect.Multiset.Entry;
	
public class WordStats {
	
	   public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
	     private  static int nline = 0;
	     private static String wsChapter = "**WordStats-Chapter:"; //separator between input chapters that would be given by a line containing a number
	     private Text word = new Text();
	     public  String tempword1 = null;
	     public  String  tempword2 = null;
	     /*
	      * Input a text line
	      * Filters all syntactic symbols dots, commas, interrogation, exclamation...
	      * Output a text line
	      */
	     public String filterSymbols(String line, String type) throws Exception {
	    	 String symbolsA = ".|,|:|;|?|!|\"\"|\"|-|_";
	    	 String[] symbols = null;
	    	 if("A".equals(type)) {
	    		 symbols = symbolsA.split("|");
	    	 }
	    	 if(!line.isEmpty()) {
	    		 for(String s:symbols) {
	    			 line = line.replace(s, " ");
	    		 
	    			 if(line.isEmpty())
	    				 break;
	    		 
	    		 }
	    	    line = line.trim();
	    	 }
	    	 return line;
	     }
	     
	    
	     public  String[] filterTokens(String[]t,String type) throws Exception {
	   	   String prepositions = "the-of";
	   	   String separator = " ";
	   	   String articles = "a-an";
	   	   String[] filter = null;
	   	   String[] ret = null;
	   	   List<String> buff = new ArrayList<String>();
	   	 
	   	   if("A".equalsIgnoreCase(type))
	   		   filter = articles.split("-");
	   	   else if("D".equalsIgnoreCase(type))
	   		   filter = prepositions.split("-");
	   	  
	   	   for(String s:t)
	   		   buff.add(s);
	   		   
	   	   
	   	   for(String f:filter) {
	   		   for(int i=0;i<buff.size();i++) {
	   			   if(buff.get(i).equalsIgnoreCase(f)) {
	   				   buff.remove(i);
	   			   }
	   		   }
	   		  
	   	   }
	   	   if(buff.size()>0) {
	   	   ret = new String[buff.size()];
	   	   for(int i=0;i<ret.length;i++)
	   		   ret[i] = buff.get(i);
	   	   }
	   	   
	   	   return ret;
	       }
	       //We suppose a text in which some words end by : and in conclu- and begin by: -sion
	     public  String[] tokenizeLine(String line) {
	   	  boolean isbeginhalfword = false;
	   	  String[] tokens = line.split(" ");
	   	  String[] ret = null;
	   	  int indexlast = tokens[tokens.length-1].indexOf("-");
	   	  int indexfirst = tokens[0].indexOf("-");
	   	  ret = new String[tokens.length];
	   	  if(indexfirst>=0 && tempword1!=null){
	   		  tempword2 = tokens[0].substring(indexfirst,tokens[0].length());
	   		  
	   		  tempword1 = tempword1+tempword2;
	   		  
	   		  for(int i=0;i<ret.length;i++) {
	   			  if(i==0)
	   				  ret[i] = tempword1;
	   			  else
	   				  ret[i]=tokens[i];
	   		  }
	   		  tempword1 = null;
	   		  return ret;
	   	  }
	   		  
	   	  if(indexlast>0) { 
	   		  tempword1 = tokens[tokens.length-1].substring(0,indexlast-1); //deleche "-" char
	   	      ret = new String[tokens.length-1];
	         }
	   	  for(int i=0; i<ret.length;i++)
	   			  ret[i] = tokens[i];
	   	  
	   		  
	   	  
	   	  return ret;
	     
	     }
	   	   
	     public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	     
	    
	    	 String line = value.toString();
	         IntWritable ln = null;
	         List<LongWritable> l = null;
	         int row = 0;
	         
	         try {
	     
	         line = filterSymbols(line,"A"); //All
	         
	         String[] tokens = tokenizeLine(line);
	         
	         tokens =  filterTokens(tokens,"D");
	         tokens = filterTokens(tokens,"A");
	     		 
	      	     
	         	 
	     		  String newChapter = null;
	     		  for(String token : tokens){
	     			 
	     			  if(tokens.length==1) {
	     				  try {
	     					  newChapter = wsChapter + Integer.valueOf(token);
	     				  }catch(NumberFormatException nfe) {
	     					  newChapter = null;
	     				  }
	     				  if(newChapter !=null)
	     					  token = newChapter;
	     			  }
	     			  
	     			
	     			  
	     			  /*     l = wmap.get(word);
	            if(l==null) {       
	          	  l = new ArrayList<LongWritable>();
	          	  
	          	  wordIdGen.addAndGet(1);
	            }
	          	  l.add(ln);
	          	  wmap.put(word,l );
	            
	         */	
	     			  if(!token.isEmpty()) {
	      		 
	     				  word = new Text();
	     				  word.set(token); 
         
	     				  Text t = new Text();
	     				  t.set(nline+";"+row);
	           
	     				  output.collect(word,t );
	          	
	     				  row++;
	     			  }    	
	     		  }
	     		  nline++;
			     
	         }catch(Exception e) {
	       	 System.err.println(e.getLocalizedMessage());
	        }
	        }
	   }
	   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
		   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
	       StringBuffer sum = new StringBuffer();
	       String sep = ",";
	       int i=0;
	       while (values.hasNext()) {
	         if(i>0)
	        	 sum.append(sep);
	    	
	         sum.append(values.next().toString());
	         i++;
	       }
	       Text t = new Text();
	       t.set(sum.toString());
	       output.collect(key, t);
	     }
	   }
	
	   public static  void applyMapReduce(String input,String outputdir) {
		  try {
		   JobConf conf = new JobConf(WordCount.class);
	       conf.setJobName("wordcount");
	
	     conf.setOutputKeyClass(Text.class);
         conf.setOutputValueClass(Text.class);
	
	     conf.setMapperClass(Map.class);
	     conf.setCombinerClass(Reduce.class);
	     conf.setReducerClass(Reduce.class);
	
	     conf.setInputFormat(TextInputFormat.class);
	     conf.setOutputFormat(TextOutputFormat.class);
	
	     FileInputFormat.setInputPaths(conf, new Path(input));
	     FileOutputFormat.setOutputPath(conf, new Path(outputdir));
	     JobClient.runJob(conf);
		  }catch(Exception ex) {
			  ex.printStackTrace();
		  }
	   }
	   //See: performance
	   public static SortedMap<String,List<RowColumn>> getResult(String filename){
		   SortedMap<String,List<RowColumn>> wordm =null;
		   String line = null;
		   String[] keyvals = null; //key-values
		   String[] rwscs = null;//values (row;cell) items
		   String[] rowcell = null;//value row cell
		   String key = null;
		   String values = null;
		   List<RowColumn> rcellslist;
		   RowColumn rcell = null;
		   BufferedReader br = null;
		   try {
		
		    wordm = new TreeMap<String,List<RowColumn>>();
		    br = new BufferedReader(new FileReader(filename));
		   while((line=br.readLine())!= null) {
			   keyvals = line.split("\\t");
			   for(int i=0;i<keyvals.length;i++) {
				   if(i==0)
					   key = keyvals[i];
				   if(i==1)
				   values = keyvals[i];
				   if(i>1)
					   throw new Exception("Exception in getting key values");
			   }
			   
			   rwscs = values.split(",");
			   String rc = null;
			   rcellslist = new LinkedList<RowColumn>();
			   for(int j=0;j<rwscs.length;j++) {
				   rc = rwscs[j];
				   rowcell = rc.split(";");
				   if(rowcell.length==2) {
					   rcell = new RowColumn(Integer.valueOf(rowcell[0]),Integer.valueOf(rowcell[1]));
					   rcellslist.add(rcell);
				   }
					   
				   
			   }
			   
			   if(rcellslist.size()>0) {
				   wordm.put(key, rcellslist);
			   }
			   
			   
			   
		   }
		   
		   
	   }catch(Exception ex) {
		   wordm=null;
		   ex.printStackTrace();
		   
	   }finally {
		   try {
		   br.close();
		   }catch(Exception ex) {
			   ex.printStackTrace();
		   }
		   
	   }
	   
	   return wordm;
	   
	   }
	   
	   public static List<WordData> getWordData(SortedMap<String,List<RowColumn>> map){
		   List<WordData> wfreqs = null;
		   WordData wd = null;
		   int size = 0;
		   WordDataHelper wordHelper = null;
		   try {
			wfreqs = new LinkedList<WordData>();
			wordHelper = new WordDataHelper();
			for(String key:map.keySet()) {
				size = map.get(key).size();
				wd = new WordData(key, Integer.valueOf(size));
				wordHelper.setWordData(wd);
				wd = wordHelper.compute();
				wfreqs.add(wd);
			}
			   
		   }catch(Exception ex) {
			   ex.printStackTrace();
		   }
	      //debug: list
		   size = 0;
		   for(WordData w:wfreqs)
			   System.out.format("%d -> %s%n",size++,w.toString());
		   
		   return wfreqs;
	   
	   }
	   
	   
	   public static void main(String[] args) throws Exception {
		   SortedMap<String,List<RowColumn>> wmap = null;
		   List<WordData> wfreqs = null;
		   XSSFWorkbook workBook = null;
		   args = new String[3];
		   args[0] = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/Chapter1-part0.txt"; 
		   args[1] = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/wordStats1";
		   String filename = "part-00000";
		
		   args[2] = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/Chapter-1.0.xlsx";
		   
		 // applyMapReduce(args[0],args[1]);
		   
		   wmap = getResult(args[1]+"/"+filename);
		   
		   wfreqs = getWordData(wmap);
	     
		   Collections.sort(wfreqs,Collections.reverseOrder());
   
		   WordData wf = wfreqs.get(0);
		   
		   workBook = OutputToXLSX.createWorkbook(args[2], 1, wf.getFrequency()*5, wf.getFrequency()*5);
		  
		   OutputToXLSX.writeBook(workBook, args[2], wmap,wfreqs);
		   
		   
	   }
}
