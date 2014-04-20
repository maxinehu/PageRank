	import java.io.IOException;
	import java.util.*;
	
	import org.apache.hadoop.fs.Path;
	import org.apache.hadoop.conf.*;
	import org.apache.hadoop.io.*;
	import org.apache.hadoop.mapred.*;
	import org.apache.hadoop.util.*;

public class PageRank {

		private static int nodeCount = 0;
		private static boolean iterationStop = false;
		
		//calculate number of edges, number of nodes, max links and min links
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
		        output.collect(new Text(id), new IntWritable(linkNum));  //collect outlinks for reduce to calculate min,max,avg for all nodes
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
			        String en = "e"+edgeNum;
			        output.collect(new Text(avgLinks), new Text(en));
				}else if(key.equals(new Text("-number of nodes:"))){
			        while (values.hasNext()) {
			        	nodeNum += values.next().get();
			        }
			        output.collect(key, new Text(nodeNum+""));
			        String nn = "n"+nodeNum;
			        output.collect(new Text(avgLinks), new Text(nn));
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
				
			}
		}
		
		//calculate avg by number of edges, number of nodes 
		public static class SummaryAllMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
			
			public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	        	    	 
				String line = value.toString();
				String[] s = line.split(":"); 		
		        output.collect(new Text(s[0].trim()+":"), new Text(s[1].trim()));
			}
		}
		
		public static class SummaryAllReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {      
			public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {			    
				String no = "";
				int en = 0;
				int nn = 0;
				if(key.equals(new Text("avg of out-degrees:"))){
			        while (values.hasNext()) {
			        	no = values.next().toString().trim();
			            if(no.startsWith("e")){
			                en = Integer.parseInt(no.substring(1));
			            }
			            if(no.startsWith("n")){
			                nn = Integer.parseInt(no.substring(1));
			            }
			        }
			        float avg = (float)en/(float)nn;
			        output.collect(key, new Text(avg+""));
				}else{
					output.collect(key, new Text(values.next()+""));
				}
				
			}
		}
		
		//format input files like "A 1.0 B C D"(pageid rank [links]) for each line for next iteration steps
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
	 	
		//read a line and map to lines of relative information
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
			     
			     output.collect(new Text(pageid), new Text("!"+rank));  //save the old rank
			     
			     while (tokenizer.hasMoreTokens()) {
			         links += " " + tokenizer.nextToken(); 					
			        }
			     
			     if(links.length()>0){
			    	 String[] linkArray = links.trim().split(" ");
			    	 int linkNum = linkArray.length;
					 for(String link : linkArray){
						 String pageRankNum = pageid + " " + rank + " " + linkNum;
						 output.collect(new Text(link), new Text(pageRankNum));   //for calculating
					 }
					 output.collect(new Text(pageid), new Text("|"+links.trim())); //We need the link so the reducer is be able to produce the correct output.
			     }
			}
	 }
	  
		//calculate new pagerank and reformat
		public static class CalculateReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {		 
		 
			private static final float damping = 0.85f;
//			private int count = 0;
			
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
		        //output former format with new ranks
		        float newRank = damping * sumRanks + (1-damping);	 
		        
//		        if(Math.abs(oldRank - newRank)<1e-4)
//		        	count++;
		        
		        out.collect(key, new Text(newRank + links));
		    
//		        if(count == nodeCount){
//		        	iterationStop = true;
//		        }
		    }
		    
		}
	 
		//because hadoop can only order ascending, so I use a thick to get the right ids
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
		        float fakeRank = 100 - Float.parseFloat(rank);   //minued by a big number, then the order is reversed.
		        output.collect(new Text(fakeRank+""), new Text(pageid));
	      }
	    }
	   
		//get the first 10 lines
	    public static class OrderReduce extends MapReduceBase implements Reducer<Text, Text, Text, Text> {
	    	private int count = 0;  
	    	public void reduce(Text key, Iterator<Text> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {	            		
		        float rank = 100 - Float.parseFloat(key+"");
	    		if(count < 10 ){
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
			runSummaryAll(args[1]+"/summary", args[1]+"/summaryAll");
			runInitialization(args[0],args[1]+"/iterative0");
//			while(iterationStop == false){
//				int a = iterCount++;
//				runCalculation(args[1]+"/iterative"+a,args[1]+"/iterative"+(a+1));
//				
//			}
			
			//10 times iteration for performance test on FutureGrid and AWS
			for(int a=0; a<5;a++){
				runCalculation(args[1]+"/iterative"+a,args[1]+"/iterative"+(a+1));
			}
			runOrdering(args[1]+"/iterative"+5,args[1]+"/DescOrder");
			
//			System.out.println(args[0]+" iterCount:" + iterCount);			
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
		
		private static void runSummaryAll(String inputPath, String outputPath) throws IOException {
		      JobConf conf = new JobConf(PageRank.class);
		      conf.setJobName("PageRank-SummaryAll");
		
		      conf.setOutputKeyClass(Text.class);
		      conf.setOutputValueClass(Text.class);
		
		      conf.setMapperClass(SummaryAllMap.class);
		      conf.setReducerClass(SummaryAllReduce.class);
		
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
