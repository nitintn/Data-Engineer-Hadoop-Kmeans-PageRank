package KMeans;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

/*
 * Partitioner Class
 */

public class KPartitioner implements Partitioner<LongWritable, Text> {

	
	public int getPartition(LongWritable key, Text value, int numReduceTasks) {

		int id = Integer.parseInt(key.toString());

		// this is done to avoid performing mod with 0
		if (numReduceTasks == 0)
			return 0;

		// if the id is 0
		if (id == 0) {
			return 0;
		}
		// else if the id is 1
		if (id == 1) {

			return 1 % numReduceTasks;
		}
		// otherwise assign partition 2
		else
			return 2 % numReduceTasks;

	}


	
	public void configure(JobConf arg0) {
		
	}
	
}
