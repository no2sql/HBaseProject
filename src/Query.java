import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.StringTokenizer;

public class Query {

	
	
	
	
	
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
			}
		}
	
	
	
	
		public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
			public void reduce(Text key, Iterator<IntWritable> values, Context context) throws IOException {
				int sum = 0;
				while (values.hasNext()) {
					sum += values.next().get();
				}
				context.write(key, new IntWritable(sum));
			}
		}
	
	
	
	
	
	
		public void stuff(String[] args) throws Exception {
			
			 Configuration conf = new Configuration();
			 Job job = new Job(conf, "wordcount");
	 	     
			 job.setOutputKeyClass(Text.class);
			 job.setOutputValueClass(IntWritable.class);
	 	
			 job.setMapperClass(Map.class);
			 job.setCombinerClass(Reduce.class);
			 job.setReducerClass(Reduce.class);	 				 
			 
			 job.setInputFormatClass(TextInputFormat.class);
			 job.setOutputFormatClass(TextOutputFormat.class);
	 	
	 	     FileInputFormat.setInputPaths(job, new Path("/input"));
	 	     FileOutputFormat.setOutputPath(job, new Path("/output"));
	 	
	 	     job.waitForCompletion(true);
	 	     
	 	   }
	
	
	
	
	
	
	public static final int NUMBER_OF_NODES = 4;
	
	static class Mapper1 extends TableMapper<Text, IntWritable> {
      
        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
  
/*            
        	  ur.productid,
        	  tr.testid,
        	  val.propertyid,
        	  tr.outcomeid
*/
        	String rowkey = Bytes.toString(row.get());
        	//System.out.println("Rowkey is " + rowkey);
        	byte salt = row.get()[0];
        	//System.out.println("Salt is " + salt);
        	String productId = new String(value.getValue(Bytes.toBytes("h"), Bytes.toBytes("pid")));
        	String testId = new String(value.getValue(Bytes.toBytes("h"), Bytes.toBytes("tid")));
        	NavigableMap<byte[], byte[]> measureFamily = value.getFamilyMap(Bytes.toBytes("h"));
        	
        	//System.out.println("productId is " + productId);
        	//System.out.println("testId is " + testId);
        	
        	try {
	        	for (byte[] columnNameBytes : measureFamily.keySet())
	        	{
	        		String columnName = Bytes.toString(columnNameBytes);
	        		if (columnName.endsWith("-gr"))
	        		{
	        			String grade = Bytes.toString(measureFamily.get(columnNameBytes));        			
	        			String measureName = columnName.substring(0, columnName.indexOf("-gr"));
	        			//System.out.println("measureName is " + measureName);
	                	//System.out.println("grade is " + grade);	                	
	        			text.set(productId + "-" + testId + "-" + measureName  + "-" + grade + "," + salt );
	        			context.write(text, ONE);	        	
	        		}
	        	}        	
        	} catch (InterruptedException e) {
                throw new IOException(e);
            }
            	       
        }
    }

    public static class Reducer1 extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //System.out.println("Reducing " + key.toString());
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            //System.out.println(String.format("stats :   key : %s,  count : %d", key.toString(), sum));
           
            context.write(null, put);
        }
    }
	
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {
    	
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)		
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));
        
			
		}

		
    	
    }
    
    public static class Mapper2 extends TableMapper<Text, IntWritable> {
        
        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
  
/*            
        	  ur.productid,
        	  tr.testid,
        	  val.propertyid,
        	  tr.outcomeid
*/
        	try {
        	String rowkey = Bytes.toString(row.get());
        	String combination = rowkey.substring(0,rowkey.indexOf(','));
        	text.set(combination);
        	context.write(text, new IntWritable(Bytes.toInt(value.getValue(Bytes.toBytes("details"), Bytes.toBytes("total")))));
        	}
        	catch (Exception e)
        	{
        		System.out.println("Error");
        		e.printStackTrace();
        	}
        	
        	            	       
        }
    }

    public static class Reducer2 extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            System.out.println("Reducing " + key.toString());
        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
            put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            //System.out.println(String.format("stats :   key : %s,  count : %d", key.toString(), sum));
           
            context.write(null, put);
        }
    }
    
    static int numComplete = 0;
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();
		
		long queryStart = System.currentTimeMillis();
		try {
			// Criteria
			//String startTime = args[0];//2011-01-28T11:23:20";
			//String endTime = args[1];//"2011-01-30T11:23:200";			
			String mode = "P";
			
			List<Job> jobList = new ArrayList<Job>();
			for (byte salt = 0; salt < NUMBER_OF_NODES; salt++)
			{
					Job job = new Job(conf,"Product-Test-Value-Outcome-Query for salt " + salt);
					jobList.add(job);
					job.setJarByClass(Query.class);     // class that contains mapper and reducer
								
					
					Scan scan = new Scan();
					scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
					scan.setCacheBlocks(false);  // don't set to true for MR jobs
					
					//scan.setStartRow(Bytes.add(Bytes.toBytes(salt), Bytes.toBytes("-" + startTime + "-" + mode)));
					//scan.setStopRow(Bytes.add(Bytes.toBytes(salt), Bytes.toBytes("-" + endTime + "-" + mode)));
							
					System.out.println("Setting scan columns...");
					//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));
					//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("TestID"));
					scan.addFamily(Bytes.toBytes("h"));
									
					
				//conf.set("mapred.job.tracker", "local");
				//conf.set("fs.default.name", "local");
				
				TableMapReduceUtil.initTableMapperJob(
						"test_skinny",      // input table
						scan,	          // Scan instance to control CF and attribute selection
						Mapper1.class,   // mapper class
						Text.class,	          // mapper output key
						IntWritable.class,	          // mapper output value
						job,
						true);
				
				job.setCombinerClass(Combiner.class);
				
				TableMapReduceUtil.initTableReducerJob(
					"kevin_result",      // output table
					Reducer1.class,             // reducer class
					job);
				
				new Thread(new JobThread(job)).start();
				
				
			}
			
			while (numComplete < NUMBER_OF_NODES)
			{
				Thread.sleep(1000);
			}
			
			System.out.println("Salts are done, aggregating...");
			
			Job job = new Job(conf,"Aggregate Nodes");
			jobList.add(job);
			job.setJarByClass(Query.class);     // class that contains mapper and reducer
						
			
			Scan scan = new Scan();
			scan.setCaching(Integer.parseInt(args[2]));        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			TableMapReduceUtil.initTableMapperJob(
					"test_skinny",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Mapper2.class,   // mapper class
					Text.class,	          // mapper output key
					IntWritable.class,	          // mapper output value
					job,
					true);
			
			TableMapReduceUtil.initTableReducerJob(
				"query_result_final",      // output table
				Reducer2.class,             // reducer class
				job);
			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			
			
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}

	public static class JobThread implements Runnable
	{
		private Job job = null;
		public JobThread(Job job)
		{
			this.job = job;
		}
		@Override
		public void run() {
			try {
			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			}
			catch (Exception e)
			{
				System.out.println("Error: " + e.getMessage());
				e.printStackTrace();
			}
			numComplete++;
			
		}
		
	}
	
}
