package com.app.wstats;

import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class WordsRecordWriter extends RecordWriter<Text, Text> {
	  private DataOutputStream out;

	  public WordsRecordWriter(DataOutputStream stream) {
	      out = stream;
	      try {
	          out.writeBytes("results:\r\n");
	      }
	      catch (Exception ex) {
	      }  
	  }

	  @Override
	  public void close(TaskAttemptContext arg0) throws IOException, InterruptedException {
	      //close our file
	      out.close();
	  }

	  @Override
	  public void write(Text k, Text v) throws IOException, InterruptedException {
	      //write out our key
	      out.writeBytes(k.toString() + ": ");
	      //loop through all values associated with our key and write them with commas between
	      //String[] linerow = (new StringBuffer().append(v.getBytes())).toString().split(");
	      out.writeBytes(v.toString());
	      out.writeBytes("\r\n");  
	  }
	  
}