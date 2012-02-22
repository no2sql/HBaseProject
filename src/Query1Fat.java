
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

public class Query1Fat extends QueryBase {

	public static final int NUMBER_OF_NODES = 4;
	
	static class Mapper1 extends TableMapper<Text, Text> {
		//private final IntWritable ONE = new IntWritable(1);
		//private Text text = new Text();
		
		private SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
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
			
			)
						
			byte salt = row.get()[0];
			byte[] startTimeBytes = Arrays.copyOfRange(row.get(), Bytes.SIZEOF_BYTE+Bytes.SIZEOF_INT, Bytes.SIZEOF_BYTE+Bytes.SIZEOF_INT+Bytes.SIZEOF_LONG);
			long startTimeLong = Bytes.toLong(startTimeBytes);
			
			cal.setTimeInMillis(startTimeLong);
			String starttime = df.format(cal.getTime());
			
			try {	        	
				context.write(new Text("sum"+salt), new Text("1"));	        	  	        	
				context.write(new Text("min"+salt), new Text(starttime));
				context.write(new Text("max"+salt), new Text(starttime));
			} catch (InterruptedException e) {
				throw new IOException(e);
			}            	       
		}
	}

    public static class Reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable> {
         
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //System.out.println("Reducing " + key.toString());
            
            Put put = new Put(Bytes.toBytes(key.toString()));
            
        	DateFormat dateformater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        	String mindatestr = new String();
        	String maxdatestr = new String();
            int sum = 0;
           	Calendar cdr = Calendar.getInstance();
           	cdr.set(2200, 0, 0);
    		long mindate = cdr.getTimeInMillis();
        	Date min = new Date(mindate);
        	cdr.set(1900, 0, 0);
    		long maxdate = cdr.getTimeInMillis();
        	Date max = new Date(maxdate);     	
            if(key.toString().contains("sum")){
            	for (Text val : values) {
    				String str = val.toString();
    				sum = sum + Integer.parseInt(str);
            	}
            	
            	put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
            } 
            if(key.toString().contains("min")){
            	String str = new String();          	
                for (Text val : values) {
                	str = val.toString();
                	Date cur;
    				try {
    					cur = dateformater.parse(str);
    					if(cur.before(min)){
    						min = cur;
    					}
    				} catch (ParseException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
                }
                mindatestr = dateformater.format(min);
                put.add(Bytes.toBytes("details"), Bytes.toBytes("min"), Bytes.toBytes(mindatestr));
            }
            if(key.toString().contains("max")){
            	String str = new String();
        		
                for (Text val : values) {
                	str = val.toString();
                	Date cur;
    				try {
    					cur = dateformater.parse(str);
    					if(cur.after(max)){
    	            		max = cur;		
    	            	}
    				} catch (ParseException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
                }
                maxdatestr = dateformater.format(max);
                put.add(Bytes.toBytes("details"), Bytes.toBytes("max"), Bytes.toBytes(maxdatestr));
            }
              
            context.write(null, put);
        }
    }
	
    public static class Combiner extends Reducer<Text, Text, Text, Text>
    {   	
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)		
				throws IOException, InterruptedException {
			DateFormat dateformater = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        	String mindatestr = new String();
        	String maxdatestr = new String();
            int sum = 0;
           	Calendar cdr = Calendar.getInstance();
           	cdr.set(2200, 0, 0);
    		long mindate = cdr.getTimeInMillis();
        	Date min = new Date(mindate);
        	cdr.set(1900, 0, 0);
    		long maxdate = cdr.getTimeInMillis();
        	Date max = new Date(maxdate);     	
            if(key.toString().contains("sum")){
            	for (Text val : values) {
    				String str = val.toString();
    				sum = sum + Integer.parseInt(str);
            	}
            	String sumstr = (new Integer(sum)).toString();
                context.write(new Text(key.toString()), new Text(sumstr));   
            } 
            if(key.toString().contains("min")){
            	String str = new String();          	
                for (Text val : values) {
                	str = val.toString();
                	Date cur;
    				try {
    					cur = dateformater.parse(str);
    					if(cur.before(min)){
    						min = cur;
    					}
    				} catch (ParseException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
                }
                mindatestr = dateformater.format(min);
                context.write(new Text(key.toString()), new Text(mindatestr));
            }
            if(key.toString().contains("max")){
            	String str = new String();
        		
                for (Text val : values) {
                	str = val.toString();
                	Date cur;
    				try {
    					cur = dateformater.parse(str);
    					if(cur.after(max)){
    	            		max = cur;		
    	            	}
    				} catch (ParseException e) {
    					// TODO Auto-generated catch block
    					e.printStackTrace();
    				}
                }
                maxdatestr = dateformater.format(max);
                context.write(new Text(key.toString()), new Text(maxdatestr));
            }          
		}    	
    }
    



    @Override
	protected void runQuery() {
    	Configuration conf = HBaseConfiguration.create();
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		long queryStart = System.currentTimeMillis();
		try {
			// Criteria				
			String mode = "P";
			
			//List<Job> jobList = new ArrayList<Job>();
		    //for (byte salt = 0; salt < NUMBER_OF_NODES; salt++)
			//{
			        //byte salt = 1;
					Job job = new Job(conf,this.getClass().getName()/* + salt*/);
					//jobList.add(job);
					job.setJarByClass(Query1Fat.class);     // class that contains mapper and reducer					
					Scan scan = new Scan();
					scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
					scan.setCacheBlocks(false);  // don't set to true for MR jobs
					
					/*
					SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
					GregorianCalendar cal = new GregorianCalendar();					 
					Date date = df.parse(args[0]);
					cal.setTime(date);
					scan.setStartRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add(new byte[] {1}, Bytes.toBytes(cal.getTimeInMillis())))));
					
					date = df.parse(args[1]);
					cal.setTime(date);
					scan.setStopRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add(new byte[] {1}, Bytes.toBytes(cal.getTimeInMillis())))));
					*/
					
					//System.out.println("Setting scan columns...");
					//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));							
				    //conf.set("mapred.job.tracker", "local");
				    //conf.set("fs.default.name", "local");
				
					scan.setFilter(new FirstKeyOnlyFilter());
					
				TableMapReduceUtil.initTableMapperJob(
						"test_fat",      // input table
						scan,	          // Scan instance to control CF and attribute selection
						Mapper1.class,   // mapper class
						Text.class,	          // mapper output key
						Text.class,	          // mapper output value
						job,
						true);
				
				job.setCombinerClass(Combiner.class);
				
				TableMapReduceUtil.initTableReducerJob(
						getResultTableName(),      // output table
					Reducer1.class,             // reducer class
					job);
				
				TableMapReduceUtil.addDependencyJars(job);
				
				boolean b = job.waitForCompletion(true);
				if (!b) {
					throw new IOException("error with job!");
				}
				//new Thread(new JobThread(job)).start();								
			//}
			
			//while (numComplete < NUMBER_OF_NODES)
			//{
			//	Thread.sleep(1000);
			//}			
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
		
	}

	static int numComplete = 0;
    
	public static void main(String[] args) throws IOException {
		
		new Query1Fat().execute();

	}

	@Override
	protected String getResultTableName() {
		return "query1_fat_result";
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

