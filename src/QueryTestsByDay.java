
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

public class QueryTestsByDay {

	public static final int NUMBER_OF_NODES = 7;
	private static final IntWritable ONE = new IntWritable(1);
	
	static class Mapper1 extends TableMapper<Text, IntWritable> {
		//private final IntWritable ONE = new IntWritable(1);
		//private Text text = new Text();
		
		private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		private GregorianCalendar cal = new GregorianCalendar();
		
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
        	long startDate = Bytes.toLong(row.get(), Bytes.SIZEOF_INT, Bytes.SIZEOF_LONG);
        	GregorianCalendar cal = new GregorianCalendar();
        	cal.setTimeInMillis(startDate);	        	
						
			String starttime = df.format(cal.getTime());
			
			try {	        	
				context.write(new Text(starttime), ONE);
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
    



    static int numComplete = 0;
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.tasktracker.map.tasks.maximum", "3");
		long queryStart = System.currentTimeMillis();
		try {
			// Criteria
			String mode = "P";
			
		
	        //byte salt = 1;
			Job job = new Job(conf,"Query test by day");			
			job.setJarByClass(QueryTestsByDay.class);     // class that contains mapper and reducer					
			Scan scan = new Scan();
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
					
			FilterList list = new FilterList(Operator.MUST_PASS_ALL);
			list.addFilter(new KeyOnlyFilter());
			list.addFilter(new FirstKeyOnlyFilter());
			scan.setFilter(list);
			//System.out.println("Setting scan columns...");
			//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));							
		    //conf.set("mapred.job.tracker", "local");
		    //conf.set("fs.default.name", "local");
		
			TableMapReduceUtil.initTableMapperJob(
					"test_skinny_live",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Mapper1.class,   // mapper class
					Text.class,	          // mapper output key
					IntWritable.class,	          // mapper output value
					job,
					true);
			
			job.setCombinerClass(Combiner.class);
			
			TableMapReduceUtil.initTableReducerJob(
				"query1_result",      // output table
				Reducer1.class,             // reducer class
				job);
			
			TableMapReduceUtil.addDependencyJars(job);
			
			
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

	
}

