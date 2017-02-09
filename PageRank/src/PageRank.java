import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

public class PageRank {
	static int iterationCount = 0;
	
	public static void main(String[] args) throws IOException 
	{
		 

		JobConf jobConf = new JobConf(PageRank.class);

		jobConf.setJobName("PageRankIterative");

		jobConf.setMapOutputKeyClass(Text.class);   
		jobConf.setMapOutputValueClass(Text.class);

		jobConf.setOutputKeyClass(Text.class);
		jobConf.setOutputValueClass(Text.class);

		jobConf.setMapperClass(PageRankMapper.class);
		jobConf.setReducerClass(PageRankReducer.class);

		jobConf.setInputFormat(TextInputFormat.class);
		jobConf.setOutputFormat(TextOutputFormat.class);
		//jobConf.setBoolean("mapred.output.compress", false);
		//jobConf.setNumReduceTasks(46);

		//TextOutputFormat.setCompressOutput(jobConf, false);
		String tempFolder = "temp/";
		FileSystem fs = FileSystem.get(jobConf);
//As maxiter=10
		while( iterationCount < 10)
		{	
			String input, output;
			if (iterationCount == 0)
				input = args[0];
			else
				input = tempFolder + iterationCount;

			output = tempFolder + (iterationCount + 1); 

			FileInputFormat.setInputPaths(jobConf, new Path(input)); // setting the input files for the job

			if(iterationCount == 9)
				FileOutputFormat.setOutputPath(jobConf, new Path(args[1] + (iterationCount + 1)));

			else
				FileOutputFormat.setOutputPath(jobConf, new Path(output)); // setting the output files for the job

			JobClient.runJob(jobConf);

			if(fs.exists(new Path(tempFolder + (iterationCount - 1))))
				fs.delete(new Path(tempFolder + (iterationCount - 1)), true);

			iterationCount++;

		}
	}

	public static class PageRankMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> 
	{

		@Override
		public void map(LongWritable number, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{
			Double pageRank = new Double(1.00d);

			if(value != null && !value.toString().isEmpty())
			{
				String [] data = value.toString().split("\t");
				String url = data[0].toString();
				
				pageRank = 0.5d; 

				Double p = 0.00d;

				if(data.length == 2)
				{
					String [] numofOutLinks = data[1].split(",");
					p = pageRank / numofOutLinks.length;

					for(String link : data[1].split(","))
					{	
						output.collect(new Text(link), new Text(String.valueOf(p)));
					}

					output.collect(new Text(url) , new Text(String.valueOf(pageRank) + "\t" + data[1]));

				}
				else if(data.length == 3)
				{
					pageRank = Double.parseDouble(data[1]);

					String [] numofOutLinks = data[2].split(",");
					p = pageRank / numofOutLinks.length;

					for(String link : data[2].split(","))
					{	
						output.collect(new Text(link), new Text(String.valueOf(p)));
					}

					output.collect(new Text(url) , new Text(String.valueOf(pageRank) + "\t" + data[2]));
				}
			}

		}

	}


	public static class PageRankReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>
	{

		@Override
		public void reduce(Text url, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException 
		{

			Double s = new Double(0.0d);
			Double finalRank = new Double(0.0d);

			
			String data = new String();
//Algorithm starts below
			while(values.hasNext())
			{
				String val = values.next().toString();


				try
				{
					Double rank;
					if((rank = Double.valueOf(val)) != null)
					{
						s = s + rank;
					}
				}
				catch(NumberFormatException e)
				{
					data = val;
				}
			}

			String [] oldDataWithRank = data.split("\t");
			//System.out.println("S is: "+s);
			finalRank = (1 - 0.85) + (0.85 * s);


			
			if (iterationCount < 9)
			{
				output.collect(url, new Text(String.valueOf(finalRank) +"\t"+ oldDataWithRank[1]));
			}
			else
			{
				output.collect(url, new Text(String.valueOf(finalRank)));
			}
		}

	}

}