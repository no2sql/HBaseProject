import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import javax.ws.rs.GET;

public class AggregateFPYLPY  {
	
	private static Map<String, Map<Integer, String>> uidMap = new HashMap<String, Map<Integer, String>>();
	private static Configuration conf = HBaseConfiguration.create();
	
	public static HTable serialNumberTable;
	
	static {
		try {
			serialNumberTable = new HTable(conf, "serial_number_ref");
						
			Scan scan = new Scan();
			HTable table = new HTable(conf, "test_uid");
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false); 
			
			scan.setStartRow(Bytes.toBytes(0));
			scan.setStopRow(Bytes.toBytes(54619));
			scan.addFamily(Bytes.toBytes("name"));
			
			ResultScanner scanner = table.getScanner(scan);
		    for (Result result : scanner) {
		    	
		    	NavigableMap<byte[], byte[]> colummns = result.getFamilyMap(Bytes.toBytes("name"));
		    	for (byte[] columnBytes : colummns.keySet())
	        	{
	        		String column = Bytes.toString(columnBytes);
	        		if (!"s".equals(columnBytes))
	        		{
		        		Map<Integer, String> columnValueMap = uidMap.get(column);
		        		if (columnValueMap == null)
		        		{
		        			columnValueMap = new HashMap<Integer, String>();
		        			uidMap.put(column, columnValueMap);
		        		}
		        
		        		columnValueMap.put(Bytes.toInt(result.getRow()), Bytes.toString(result.getValue(Bytes.toBytes("name"), columnBytes)));
	        		}
	        	}
		    }
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	}
	
	
	public static class Mapper1 extends TableMapper<BytesWritable, IntWritable> {
      
		
		
		
        private final IntWritable ONE = new IntWritable(1);
        private BytesWritable combo = new BytesWritable();
        private Text text = new Text();
        
        public Mapper1(){
        	
        }
        
        public static long mapTime = 0;
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
        	
        	try {
	        	int serialNumberId = Bytes.toInt(row.get());
	            int productId = Bytes.toInt(value.getValue(Bytes.toBytes("details"), Bytes.toBytes('p')));
	            
	            GregorianCalendar cal = new GregorianCalendar();
	            
	            NavigableMap <byte[], byte[]> familyMap = value.getFamilyMap(Bytes.toBytes("details"));
	            
	            for (byte[] columnBytes : familyMap.keySet())
	            {
	            	if (columnBytes.length == Bytes.SIZEOF_INT*2){
		            	int testId = (int) Bytes.toInt(columnBytes, 0 , Bytes.SIZEOF_INT);
		            	
			            char startChar = (char) Bytes.toInt(columnBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
			            byte[] valBytes = familyMap.get(columnBytes);
			            long startTime = Bytes.toLong(valBytes, 0, Bytes.SIZEOF_LONG);
			            
			            int gradeId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
			            
			            byte isParent = valBytes[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
			            
			            int stationId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
			            int operatorId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT*2, Bytes.SIZEOF_INT);
			            cal.setTimeInMillis(startTime);
			            cal.set(Calendar.HOUR, 0);
			        	cal.set(Calendar.MINUTE, 0);
			        	cal.set(Calendar.SECOND, 0);
			            //if (stationId != 1)
			            //System.out.println("\t" + testId + " " + startChar + " " + df.format(cal.getTime()) + " " + gradeId + " " + isParent + " " + stationId + " " + operatorId);
			        	byte[] aggId = Bytes.add(Bytes.toBytes(cal.getTimeInMillis()), Bytes.add(new byte[] {isParent}, Bytes.add(Bytes.toBytes(testId), Bytes.add(Bytes.toBytes(productId), Bytes.add(Bytes.toBytes(stationId), Bytes.add(Bytes.toBytes(operatorId), Bytes.add(Bytes.toBytes(startChar), Bytes.toBytes(gradeId))))))));
			        	context.write(new BytesWritable(aggId), ONE); // PASS and FAIL count
	            	}
	            }
        	
        		
        	} catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        
    }

	 public static class Combiner extends Reducer<BytesWritable, IntWritable, BytesWritable, IntWritable>
	    {
	    	
			@Override
			public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context)		
					throws IOException, InterruptedException {
				int sum = 0;
				for (IntWritable val : values) {
	                sum += val.get();
	            }

	            context.write(key, new IntWritable(sum));        			
			}
	    	
	    }
	    
	
    public static class Reducer1 extends TableReducer<BytesWritable, IntWritable, BytesWritable> {

        public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            Put put = new Put(Bytes.head(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 4));
            char type = (char) Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 4, Bytes.SIZEOF_INT);
            
        	int gradeId = Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 5, Bytes.SIZEOF_INT);
        	put.add(Bytes.toBytes("details"), Bytes.add(Bytes.toBytes(type),Bytes.toBytes(gradeId)), Bytes.toBytes(sum));	
         
            context.write(null, put);
        }
    }
	
   
    
    
	/**
	 * @param args
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException, ParseException {
		
		if (args.length > 0)
			new AggregateFPYLPY(args[0], args[1]).execute();
		else
			new AggregateFPYLPY(null, null).execute();		
	}
    
    public void execute() throws IOException
	{		
		long queryStart = System.currentTimeMillis();
				
		try { 
		
			createJob();
			
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private void createJob() throws IOException, InterruptedException, ClassNotFoundException
	{
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		//conf.set("mapred.task.profile", "true");
		//System.out.println("AM profiling");
		conf.set("hadoop.job.history.user.location", "/mapred");
		//conf.set("mapred.task.profile.maps", "4");
		//conf.set("mapred.task.profile.reduces", "4");
		
		Job job = new Job(conf, this.getClass().getName() );
		
		job.setJarByClass(this.getClass());     // class that contains mapper and reducer					
		Scan scan = new Scan();
		scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		//System.out.println("Setting scan columns...");
		//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));							
	    //conf.set("mapred.job.tracker", "local");
	    //conf.set("fs.default.name", "local");
	
		setScanFilter(scan);
		
		TableMapReduceUtil.initTableMapperJob(
				getInputTableName(),      // input table
				scan,	          // Scan instance to control CF and attribute selection
				getMapperClass(),   // mapper class
				getMapOutputKeyClass(),	          // mapper output key
				getMapOutputValueClass(),	          // mapper output value
				job,
				true);
		
		if (getCombinerClass() != null)
			job.setCombinerClass(getCombinerClass());
		
		if (getReducerClass() != null)
		TableMapReduceUtil.initTableReducerJob(getResultTableName(),
				getReducerClass(), job,
				null, null, null,
				null, false);
		
		//TableMapReduceUtil.addDependencyJars(job);
		
		
		boolean b = job.waitForCompletion(true);
	}
	
    
    
    
    public AggregateFPYLPY(String startDate, String endDate) throws ParseException
	{
		
	}

	protected void setScanFilter(Scan scan) {
		
	}

	protected Class<? extends TableMapper> getMapperClass() {
		return Mapper1.class;
	}

	protected Class<? extends Reducer> getCombinerClass() {
		return Combiner.class;
	}

	protected Class<? extends TableReducer> getReducerClass() {
		return Reducer1.class;
	}

	protected Class<? extends TableMapper> getAggregationMapperClass() { 
		return null;
	}

	protected Class<? extends TableReducer> getAggregationReducerClass() {
		return Reducer1.class;
	}

	protected Class getMapOutputKeyClass() {
		return BytesWritable.class;
		//return Text.class;
	}

	protected Class getMapOutputValueClass() {
		return IntWritable.class;
	}


	protected String getResultTableName() {
		return "aggregate";
	}

	protected String getInputTableName() {
		return "serial_number_ref";
	}

	
	
}
