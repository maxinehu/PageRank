	import java.io.IOException;
	import java.util.*;
	import java.nio.ByteBuffer;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class PageRank {

		private static int nodeCount = 0;
		private static int top = 10;
		private static boolean iterationStop = false;
	 
		public static class SummaryMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
			private final static IntWritable one = new IntWritable(1);
			public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {	        	    	 
				String pageid = "";
			    String links = "";
			    int linkNum=0;
		    	String line = value.toString();				
		        StringTokenizer tokenizer = new StringTokenizer(line); 		
		        if(tokenizer.hasMoreTokens())
		        	pageid = tokenizer.nextToken();
		        while (tokenizer.hasMoreTokens()) {
		          links += " " + tokenizer.nextToken();
		          linkNum++;
		        }
		        String id = "id";
		        output.collect(new Text(id), new IntWritable(linkNum));
		        String ne = "-number of edges:";
		        output.collect(new Text(ne), new IntWritable(linkNum));
		        String nn = "-number of nodes:";
		        output.collect(new Text(nn), one);
			}
		}
	
		public static class SummaryReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {      
			int nodeNum = 0;
			int edgeNum = 0;
			public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			    
				String maxLinks = "max of out-degrees:";
				String minLinks = "min of out-degrees:";
				String avgLinks = "avg of out-degrees:";
				int min = 0;
				int max = 0;
				float avg = 0.0f;

				if(key.equals(new Text("-number of edges:"))){
			        while (values.hasNext()) {
			        	edgeNum += values.next().get();
			        }
			        output.collect(key, new Text(edgeNum+""));
				}else if(key.equals(new Text("-number of nodes:"))){
			        while (values.hasNext()) {
			        	nodeNum += values.next().get();
			        }
			        output.collect(key, new Text(nodeNum+""));
				}else{
					while (values.hasNext()) {
						int next = values.next().get();
						if(max < next)
							max = next;
						if(min > next)
							min = next;
			        }
					output.collect(new Text(maxLinks), new Text(max+""));
					output.collect(new Text(minLinks), new Text(min+""));
				}
				
				if((nodeNum != 0)&&(edgeNum != 0)){
					avg = (float)edgeNum/(float)nodeNum;
					output.collect(new Text(avgLinks), new Text(avg+""));
					nodeCount = nodeNum;
					nodeNum = 0;//ensure avg can be generate only once
				}
				
			}
		}
	 
		public static class InitialMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {	      
			private final static FloatWritable initialRank = new FloatWritable(1.0f);      
			public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	       		    
				String pageid = "";
			    String links = "";
		    	String line = value.toString();				
		        StringTokenizer tokenizer = new StringTokenizer(line); 		
		        if(tokenizer.hasMoreTokens())
		        	pageid = tokenizer.nextToken();
		        while (tokenizer.hasMoreTokens()) {
		          links += " " + tokenizer.nextToken(); 					
		        }
		        output.collect(new Text(pageid), new Text(initialRank + links));	      
			}
	    }
	 	
		public static class CalculateMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {		 
		 
			public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			 
				String pageid = "";
				 float rank = 0.0f;
				 String links = "";
				 String line = value.toString();				
			     StringTokenizer tokenizer = new StringTokenizer(line); 		
			     
			     //get pageid
			     if(tokenizer.hasMoreTokens())
			      	 pageid = tokenizer.nextToken();
			     //get rank
			     if(tokenizer.hasMoreTokens())
			    	 rank = Float.parseFloat(tokenizer.nextToken());
			     
			     output.collect(new Text(pageid), new Text("!"+rank));
			     
			     while (tokenizer.hasMoreTokens()) {
			         links += " " + tokenizer.nextToken(); 					
			        }
			     
			     if(links.length()>0){
			    	 String[] linkArray = links.trim().split(" ");
			    	 int linkNum = linkArray.length;
					 for(String link : linkArray){
						 String pageRankNum = pageid + " " + rank + " " + linkNum;
						 output.collect(new Text(link), new Text(pageRankNum));
					 }
					 output.collect(new Text(pageid), new Text("|"+links.trim()));
			     }
			}
	 }
	  
		public static class CalculateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {		 
		 
			private static final float damping = 0.85f;
			private int count = 0;
			
		    public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> out, Reporter reporter) throws IOException {
		        String[] split;
		        float sumRanks = 0.0f;
		        float oldRank = 0.0f;
		        String links = "";
		        String pageidFollow;
		 
		        while(values.hasNext()){
		            pageidFollow = values.next().toString();
		            //get old pagerank
		            if(pageidFollow.startsWith("!")){
		                oldRank = Float.parseFloat(pageidFollow.substring(1));
		                continue;
		            }
		            //get links
		            if(pageidFollow.startsWith("|")){
		                links = " "+pageidFollow.substring(1);
		                continue;
		            }
		 
		            split = pageidFollow.split(" ");
		 
		            float pageRank = Float.valueOf(split[1]);
		            int countOutLinks = Integer.valueOf(split[2]);
		 
		            sumRanks += (pageRank/countOutLinks);
		        }
		 
		        float newRank = damping * sumRanks + (1-damping);	 
		        
		        if(Math.abs(oldRank - newRank)<1e-4)
		        	count++;
		        
		        out.collect(key, new Text(newRank + links));
		    
		        if(count == nodeCount){
		        	iterationStop = true;
		        }
		    }
		    
		}
	 
		public static class OrderMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {	      	      
			public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	        		    
				String pageid = "";
			    String rank = "";
		    	String line = value.toString();				
		        StringTokenizer tokenizer = new StringTokenizer(line); 		
		        if(tokenizer.hasMoreTokens())
		        	pageid = tokenizer.nextToken();
		        if(tokenizer.hasMoreTokens()) {
		        	rank = tokenizer.nextToken(); 					
		        }
		        float fakeRank = 100 - Float.parseFloat(rank);
		        output.collect(new Text(fakeRank+""), new Text(pageid));
	      }
	    }
	   
	    public static class OrderReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    	private int count = 0;  
	    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	            		
		        float rank = 100 - Float.parseFloat(key+"");
	    		if(count < top ){
		        	while (values.hasNext()) {
				          Text value = values.next();
				          output.collect(new Text(rank+""), value);
			        }
		        }
	    		count++;
		      }   
	    }
	    		
		public static void main(String[] args) throws Exception {
			
			int iterCount = 0;
			runSummary(args[0],args[1]+"/summary");
			runInitialization(args[0],args[1]+"/iterative0");
			while(iterationStop == false){
				int a = iterCount++;
				runCalculation(args[1]+"/iterative"+a,args[1]+"/iterative"+(a+1));
				
			}
			runOrdering(args[1]+"/iterative"+iterCount,args[1]+"/DescOrder");
			System.out.println(args[0]+" iterCount:" + iterCount);			
	    }
	
		private static void runSummary(String inputPath, String outputPath) throws IOException {
		      JobConf conf = new JobConf(PageRank.class);
		      conf.setJobName("PageRank-Summary");
		
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(IntWritable.class);
		
		      conf.setMapperClass(SummaryMap.class);
		      conf.setReducerClass(SummaryReduce.class);
		
		      conf.setInputFormat(TextInputFormat.class);
		      conf.setOutputFormat(TextOutputFormat.class);
		
		      FileInputFormat.setInputPaths(conf, new Path(inputPath));
		      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		      JobClient.runJob(conf);
	    }

		private static void runInitialization(String inputPath, String outputPath) throws IOException {
		      JobConf conf = new JobConf(PageRank.class);
		      conf.setJobName("PageRank-Job1");
		
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(Text.class);
		
		      conf.setMapperClass(InitialMap.class);
		
		      conf.setInputFormat(TextInputFormat.class);
		      conf.setOutputFormat(TextOutputFormat.class);
		
		      FileInputFormat.setInputPaths(conf, new Path(inputPath));
		      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		      JobClient.runJob(conf);
	    }

		private static void runCalculation(String inputPath, String outputPath) throws IOException {
		      JobConf conf = new JobConf(PageRank.class);
		      conf.setJobName("PageRank-Job2");
		
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(Text.class);
		
		      conf.setMapperClass(CalculateMap.class);
		      conf.setReducerClass(CalculateReduce.class);
		
		      conf.setInputFormat(TextInputFormat.class);
		      conf.setOutputFormat(TextOutputFormat.class);
		
		      FileInputFormat.setInputPaths(conf, new Path(inputPath));
		      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		      JobClient.runJob(conf);
	    }

		private static void runOrdering(String inputPath, String outputPath) throws IOException {
		      JobConf conf = new JobConf(PageRank.class);
		      conf.setJobName("PageRank-Job3");
		
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(Text.class);
		
		      conf.setMapperClass(OrderMap.class);
		      conf.setReducerClass(OrderReduce.class);
		      
		      conf.setInputFormat(TextInputFormat.class);
		      conf.setOutputFormat(TextOutputFormat.class);		      
	      
		      FileInputFormat.setInputPaths(conf, new Path(inputPath));
		      FileOutputFormat.setOutputPath(conf, new Path(outputPath));
		
		      JobClient.runJob(conf);
	    }
	 
}
