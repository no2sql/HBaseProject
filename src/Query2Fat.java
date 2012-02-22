import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public class Query2Fat extends QueryBase {

	public static final int NUMBER_OF_NODES = 4;
	
	static class Mapper1 extends TableMapper<Text, IntWritable> {
      
        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();
        
        private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
		private GregorianCalendar cal = new GregorianCalendar();
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
  
/*            
        	  ur.productid,
        	  tr.testid,
        	  val.propertyid,
        	  tr.outcomeid
*/
        	
        	//context.getCounter("camstar","RowInputCounter").increment(1l);
        	
        	//System.out.println("Rowkey is " + rowkey);        	
        	//System.out.println("Salt is " + salt);
        	
        
        	
        	try {
        		byte[] startTimeBytes = Arrays.copyOfRange(row.get(), Bytes.SIZEOF_BYTE+Bytes.SIZEOF_INT, Bytes.SIZEOF_BYTE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG);
    			long startTimeLong = Bytes.toLong(startTimeBytes);
    			
    			cal.setTimeInMillis(startTimeLong);
    			String starttime = df.format(cal.getTime());
	        			
    			String grade = new String(value.getValue(Bytes.toBytes("h"), Bytes.toBytes("gr")));	                
            	
    			text.set(starttime + "-" + grade );
    			context.write(text, ONE);
        		        	
        		
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
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		new Query2Fat().execute();

	}

	@Override
	protected void runQuery() {
		
		Configuration conf = HBaseConfiguration.create();
		
		long queryStart = System.currentTimeMillis();
		try {
			// Criteria
							
			conf.set("mapred.map.tasks.speculative.execution", "false");
			Job job = new Job(conf, this.getClass().getName() );				
			job.setJarByClass(Query2Fat.class);     // class that contains mapper and reducer							
							
			Scan scan = new Scan();
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
				
			System.out.println("Setting scan columns...");
			scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("gr"));
			//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("TestID"));
			//scan.addFamily(Bytes.toBytes("m"));
			//scan.addFamily(Bytes.toBytes("h"));
			//scan.addFamily(Bytes.toBytes("mr"));
				
			//conf.set("mapred.job.tracker", "local");
			//conf.set("fs.default.name", "local");
			//conf.setInt("mapred.job.reuse.jvm.num.tasks",-1);
			//conf.setInt("mapred.tasktracker.map.tasks.maximum",10);
			
			TableMapReduceUtil.initTableMapperJob(
					"test_fat",      // input table
					scan,	          // Scan instance to control CF and attribute selection
					Mapper1.class,   // mapper class
					Text.class,	          // mapper output key
					IntWritable.class,	          // mapper output value
					job,
					true);
			
			job.setCombinerClass(Combiner.class);
			
			TableMapReduceUtil.initTableReducerJob(
				getResultTableName(),      // output table
				Reducer1.class,             // reducer class
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

	@Override
	protected String getResultTableName() {
		return "query2_fat_result";
	}

	
	
}
