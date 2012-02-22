
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.FilterList.Operator;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public class QueryUnitReportsByDay {

	public static final int NUMBER_OF_NODES = 7;
	private static final IntWritable ONE = new IntWritable(1);
	
	static class Mapper1 extends TableMapper<Text, IntWritable> {
		//private final IntWritable ONE = new IntWritable(1);
		//private Text text = new Text();
		
		private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		private GregorianCalendar cal = new GregorianCalendar();
		
		private static int count = 0;
		
		@Override
		public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {         
			//ur.starttime
			//String rowkey = Bytes.toString(row.get());
			//System.out.println("Row key is " + rowkey);
			/*
			if (rowkey.contains("-P-0-"))
				context.getCounter("camstar","subtest").increment(1l);
			
			if (rowkey.contains("-P-1-"))
				context.getCounter("camstar","toptest").increment(1l);
			*/
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        	long startDate = Bytes.toLong(row.get(), Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_LONG);
        	char mode = (char) Bytes.toInt(row.get(), Bytes.SIZEOF_BYTE, Bytes.SIZEOF_INT);
        	GregorianCalendar cal = new GregorianCalendar();
        	cal.setTimeInMillis(startDate);	        	
						
			String starttime = df.format(cal.getTime());
			if (count++ % 100000 == 0)
			{
				System.out.println("Rowkey is ");
				for (byte b : row.get())
				{
					System.out.print(b + " ");
				}
				System.out.println();
			}
			try {	        	
				context.write(new Text(starttime + "," + row.get()[0] + "-" + mode), ONE);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}            	       
		}
	}

    public static class Reducer1 extends TableReducer<Text, IntWritable, ImmutableBytesWritable> {
         
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            //System.out.println("Reducing " + key.toString());
            
            Put put = new Put(Bytes.toBytes(key.toString()));
            
            int sum = 0;
			
			for (IntWritable val : values)
				sum += val.get();
					
			put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
              
            context.write(null, put);
        }
    }
	
    public static class Combiner extends Reducer<Text, IntWritable, Text, IntWritable>
    {   	
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)		
				throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values)
				sum += val.get();
					
            context.write(new Text(key.toString()), new IntWritable(sum));
                      
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
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		long queryStart = System.currentTimeMillis();
		char[] modeList = new char[]{'D','P'};
		
		try {
			// Criteria
			
			
			for (char mode : modeList)
			//for (char mode : new char[]{'P'})
			{
				
				List<Job> jobList = new ArrayList<Job>();
				for (byte salt = 0; salt < NUMBER_OF_NODES; salt++)
				//for (byte salt = 0; salt < 1; salt++)
				{
						Job job = new Job(conf,"Unit Reports by Day, mode: " + mode + " salt: " + salt);
						jobList.add(job);
						job.setJarByClass(Query.class);     // class that contains mapper and reducer
									
						Scan scan = new Scan();
						scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
						scan.setCacheBlocks(false);  // don't set to true for MR jobs
						
						scan.setStartRow(Bytes.add(new byte[]{salt}, Bytes.add(Bytes.toBytes(mode), new byte[]{1})));
						scan.setStopRow(Bytes.add(new byte[]{salt}, Bytes.add(Bytes.toBytes(mode), new byte[]{2})));
								
						System.out.println("Setting scan columns...");
						//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));
						//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("TestID"));
						//scan.addFamily(Bytes.toBytes("h"));
						FilterList list = new FilterList(Operator.MUST_PASS_ALL);
						list.addFilter(new KeyOnlyFilter());
						list.addFilter(new FirstKeyOnlyFilter());
						scan.setFilter(list);
						
					//conf.set("mapred.job.tracker", "local");
					//conf.set("fs.default.name", "local");
					
					TableMapReduceUtil.initTableMapperJob(
							"test_skinny_id_bigger",      // input table
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
			
			}
			while (numComplete < NUMBER_OF_NODES*modeList.length)
			{
				Thread.sleep(1000);
			}
			
			System.out.println("Salts are done, aggregating...");
			
			Job job = new Job(conf,"Aggregate Nodes");		
			job.setJarByClass(QueryUnitReportsByDay.class);     // class that contains mapper and reducer
						
			
			Scan scan = new Scan();
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			TableMapReduceUtil.initTableMapperJob(
					"kevin_result",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Mapper2.class,   // mapper class
					Text.class,	          // mapper output key
					IntWritable.class,	          // mapper output value
					job,
					true);
			
			TableMapReduceUtil.initTableReducerJob(
				"kevin_result_final",      // output table
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

