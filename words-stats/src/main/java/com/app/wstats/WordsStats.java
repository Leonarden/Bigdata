package com.app.wstats;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


import com.app.wstats.WordCount.Reduce;

public class WordsStats {
	//This map will store words as keys and a list of LINEROW of lines in which word is repeated, frequency is given by list.size()
	
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
     private Text word = null;
   //  public static SortedMap<Text,List<LongWritable>> wmap = new TreeMap<Text,List<LongWritable>>();
     public  String tempword1 = null;
     public  String  tempword2 = null;
 	public  AtomicLong wordIdGen = new AtomicLong();
 	public static AtomicInteger lineIdGen = new AtomicInteger();
     
   
     public  String[] filter(String[]t,String type) throws Exception {
 	   String determinants = "the-of";
 	   String separator = " ";
 	   String articles = "a-an";
 	   String[] filter = null;
 	   String[] ret = null;
 	   List<String> buff = new ArrayList<String>();
 	 
 	   if("A".equalsIgnoreCase(type))
 		   filter = articles.split("-");
 	   else if("D".equalsIgnoreCase(type))
 		   filter = determinants.split("-");
 	  
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
       IntWritable row = null;
       
       try {
       	 
       String[] tokens = tokenizeLine(line);
        
       tokens =  filter(tokens,"D");
       tokens = filter(tokens,"A");
   		  if(tokens.length>0) {
    	   ln = new IntWritable(lineIdGen.addAndGet(1));
   		  	row = new IntWritable(0);
   		  }
   		  int i = 0;
   		  for(String token : tokens){
          word = new Text();
          word.set(token); 
     /*     l = wmap.get(word);
          if(l==null) {       
        	  l = new ArrayList<LongWritable>();
        	  
        	  wordIdGen.addAndGet(1);
          }
        	  l.add(ln);
        	  wmap.put(word,l );
          
       */	
    	 
         Text t = new Text();
         t.set(""+ln+";"+row);
         
         output.collect(word,t );
         
         i++;
         row = new IntWritable(i);
   		 }
      }catch(Exception e) {
     	 System.err.println(e.getLocalizedMessage());
      }
    
     
   }

   public static class Reduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	
	   public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
       //List<LongWritable> lines = new LinkedList<LongWritable>();
       StringBuffer sb = new StringBuffer();
       String separator = ",";
       Text out = new Text();
       int i=0;
       while (values.hasNext()) {
         if(i>0)
        	 sb.append(separator);
         sb.append(values.next().toString());
         i++;
       }
       out.set(sb.toString());
       output.collect(key, out);
	   }
	}
   }

   public static void main(String[] args) throws Exception {
     args = new String[2];
	 args[0] = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/wordsIn.txt";
	 args[1] = "/home/david/.wrk/user/dev/workspaces/5.0/worksbigd/words-stats/words-stats/src/main/java/resources/wordsOut";
	 JobConf conf = new JobConf(WordsStats.class);
     conf.setJobName("wordsstats");

     conf.setOutputKeyClass(Text.class);
     conf.setOutputValueClass(Text.class);

     conf.setMapperClass(Map.class);
     conf.setCombinerClass(Reduce.class);
     conf.setReducerClass(Reduce.class);

     conf.setInputFormat(TextInputFormat.class);
     conf.setOutputFormat(TextOutputFormat.class);

     FileInputFormat.setInputPaths(conf, new Path(args[0]));
     FileOutputFormat.setOutputPath(conf, new Path(args[1]));

     JobClient.runJob(conf);
     
 /*    if(wmap==null) {
    	 System.out.println("Failed");
    	 System.exit(1);
     }
   */  
     System.exit(0);
}}
   

