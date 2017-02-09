package KMeans;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;


/*
 * Driver class  
 */

public class KDriver {
	private HashMap<String, String> centroidList = new HashMap<String,String>();
	private int jobCount = 1;
	//public String CENTROID_FILE_NAME = "/centroid.txt";

	public static void main(String[] args) throws IOException, ParseException {
		
		KDriver obj = new KDriver();
		
		
		int linescount = 0;
		
		if(args.length == 2){
			/*
			 * Read the centroid file into a Hashmap
			 */
			
			//args 0-centroid
			//args 1 kmeans data file
			try{
				Path pt=new Path("InitialCentroid.txt");
		        FileSystem fs = FileSystem.get(new Configuration());
		        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(pt)));
		        String line;
	
		        while ((line = bufferReader.readLine()) != null)   {
		        	linescount = linescount+1;
		        	String []keyVal = line.split("\t");
		      		obj.centroidList.put(keyVal[0], keyVal[1]);
		        }
		        bufferReader.close();
		    }
		    catch(Exception e){
		    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
		    }
			
			/*
			 * 	Iterative jobs for the convergence of the centroids, meaning that
			 *	the change of centroids between current iteration and last iteration is less
			 *	than 0.01)
			 */
			int iterationCount = 0;						
			 
			if(linescount >=3 && linescount <=8){
				while(obj.jobCount != 0){
					JobClient client2 = new JobClient();
					JobConf conf2 = new JobConf(KDriver.class);
					
					conf2.setMapperClass(KMapper.class);
					conf2.setPartitionerClass(KPartitioner.class);
					conf2.setReducerClass(KReducer.class);
					conf2.setNumReduceTasks(3);
				
					//Set the output types for mapper and reducer Class
					conf2.setMapOutputKeyClass(Text.class);
					conf2.setMapOutputValueClass(Text.class);
					conf2.setOutputKeyClass(Text.class);
					conf2.setOutputValueClass(Text.class);
					
					String input, output;	
						
					input = args[0];	
					
					output = args[1] + (iterationCount + 1);		
					FileInputFormat.setInputPaths(conf2, new Path(input));
					FileOutputFormat.setOutputPath(conf2, new Path(output));
					client2.setConf(conf2);
						
					try {
						JobClient.runJob(conf2);
					} 
					catch (Exception e) {
						e.printStackTrace();
					}
					
					obj.jobCount--;
					
					
					HashMap<String, String> newCentroidList = new HashMap<String,String>();
					
					
					try{   	
						//copyMerge(FileSystem srcFS,Path srcDir,FileSystem dstFS, Path dstFile, boolean deleteSource,conf2, String addString)
						String src = output;
						//String temp = "out"+iterationCount;
						String dest = output+"/final.txt";
						//String dest = "/user/root/"+output;
						Path pt=new Path(src);
						Path dstFile = new Path(dest);
				        FileSystem fs = FileSystem.get(conf2);
				        FileUtil.copyMerge(fs,pt,fs, dstFile, false,conf2, null);
				        //BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(dstFile)));
				        BufferedReader bufferReader = new BufferedReader(new InputStreamReader(fs.open(dstFile), "UTF-8"));
				        String line;
				        while ((line = bufferReader.readLine()) != null) {
				        	String []keyVal = line.split("\t");
				      		newCentroidList.put(keyVal[0], keyVal[1]);
				        }
				        bufferReader.close();
				    }
				    catch(Exception e){
				    	System.out.println("Error while reading file line by line:" + e.getMessage());                      
				    }	
					
					
					/*
					 * Check if the the change of centroid from the current iteration 
					 * is greater than 0.01 to continue the iteration
					 */
					int counter=0;
					for(Map.Entry<String, String> entry : newCentroidList.entrySet()){
						String result = obj.centroidList.getOrDefault(entry.getKey(), "N");
						if(!(result.equalsIgnoreCase("N"))){
							String[] newVal = entry.getValue().split(",");
							String[] oldVal = result.split(",");
							Double sum =0.0;
							Double Edist = 0.0;
							for(int i =0; i<oldVal.length; i++){
								double x1 = Double.parseDouble(newVal[i]);
								double y1 = Double.parseDouble(oldVal[i]);
								double  xDiff = x1-y1;
						        double  xSqr  = Math.pow(xDiff, 2);
								sum+=xSqr;
							}
							Edist = Math.sqrt(sum);
							if(Edist>0.01){
								counter++;
							}
						}
					}
					if(counter >0){
						System.out.println("Yes");
						obj.centroidList.clear();
						Path pt1=new Path("InitialCentroid.txt");
				        FileSystem fs = FileSystem.get(new Configuration());
						
				        
				        ArrayList<String> sb=new ArrayList<String>();
				        FSDataOutputStream fsOutStream = fs.create(pt1, true);
				        BufferedWriter br = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
				        
						for(Map.Entry<String, String> entry1 : newCentroidList.entrySet()){
							obj.centroidList.put(entry1.getKey(), entry1.getValue());
							sb.add(entry1.getKey()+"\t"+entry1.getValue());
						}
						for(String temp:sb){
							br.write(temp+"\n");
						}
						
						br.close();
						
						obj.jobCount++;
					}
					iterationCount++;
				}
				Path pt2=new Path("InitialCentroid.txt");
				FileSystem fs = FileSystem.get(new Configuration());
				FSDataOutputStream fsOutStream = fs.create(pt2, true);
				BufferedWriter br = new BufferedWriter( new OutputStreamWriter(fsOutStream, "UTF-8" ) );
				//br.write("Hello World");
				br.write("1	1.821857,13.9764,7.647539,1.897658,8.437145,9.691624,8.878907,4.473058,13.55891,7.949216"+"\n");
				br.write("2	11.62465,5.775332,5.919049,10.71918,10.10611,8.969507,10.59489,9.910682,6.372825,-3.946758"+"\n");
				br.write("3	9.221022,2.646238,7.60925,12.08971,16.7934,9.486061,11.93836,9.730975,3.114702,7.925027"+"\n");
				br.write("4	11.50013,9.815879,5.177382,4.733192,9.484817,8.783316,2.556222,2.462524,7.82291,9.842665"+"\n");
								br.close();
			}
			else
			{
				System.out.println("Atleast 3 and a maximum of 8 cenroids allowed!!! ");
			}
		}
		else
		{
			System.out.println("Invalid command line arguments!!!");
		}
	}

}
