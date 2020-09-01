package com.app.wstats;



import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.Progressable;


public class WordsFileOutputFormat extends FileOutputFormat<Text, Text> {
    public org.apache.hadoop.mapreduce.RecordWriter<Text, Text> getRecordWriter(JobConf ta) throws IOException, InterruptedException {
       //get the current path
       org.apache.hadoop.fs.Path path = FileOutputFormat.getOutputPath(ta);
       //create the full path with the output directory plus our filename
       Path fullPath = new Path(path, "result.txt");
   //create the file in the file system
   FileSystem fs = path.getFileSystem(ta);
   FSDataOutputStream fileOut = fs.create(fullPath);

   //create our record writer with the new file
   return new WordsRecordWriter(fileOut);
}

	@Override
	public org.apache.hadoop.mapred.RecordWriter<Text, Text> getRecordWriter(FileSystem arg0, JobConf arg1, String arg2,
			Progressable arg3) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}



}


