import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;

public class SerialNumberRef extends QueryBase {
	
	private static Map<String, Map<Integer, String>> uidMap = new HashMap<String, Map<Integer, String>>();
	private static Configuration conf = HBaseConfiguration.create();
	
	static {
		try {
			
						
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
	
	public static class Mapper1 extends TableMapper<BytesWritable, LongWritable> {
      
		
		
		
        private Text text = new Text();
        
        public Mapper1(){
        	
        }
        
        public static long mapTime = 0;
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
        	        
        	NavigableMap<byte[], byte[]> family = value.getFamilyMap(Bytes.toBytes("h"));
        	
        	GregorianCalendar cal = new GregorianCalendar();
        	cal.setTimeInMillis(Bytes.toLong(row.get(), Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_LONG));
        	        	
        	int productId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('p')));
        	int testId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('t')));
        	int serialNumberId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('s')));
        	
			try {
		
				context.write(new BytesWritable(Bytes.add(Bytes.toBytes(serialNumberId), Bytes.add(Bytes.toBytes(productId), Bytes.toBytes(testId)))), new LongWritable(cal.getTimeInMillis())); 		    
        		
        	} catch (InterruptedException e) {
                throw new IOException(e);
            }
        }
        
    }

	 public static class Combiner extends Reducer<BytesWritable, LongWritable, BytesWritable, LongWritable>
	    {
	    	
			@Override
			public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)		
					throws IOException, InterruptedException {
				Set<Long> prevValues = new HashSet<Long>();
				for (LongWritable val : values) {
					if (!prevValues.contains(val.get()))
					{
						context.write(key, val);
					}
	            }

	                    			
			}
	    	
	    }
	    
	
    public static class Reducer1 extends TableReducer<BytesWritable, LongWritable, ImmutableBytesWritable> {

        public void reduce(BytesWritable key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
        	
        	Put put = new Put(Bytes.head(key.getBytes(), Bytes.SIZEOF_INT)); // Serial Number ID
            put.add(Bytes.toBytes("details"), Bytes.toBytes('p'), Bytes.toBytes(Bytes.toInt(key.getBytes(), Bytes.SIZEOF_INT, Bytes.SIZEOF_INT))); // Product ID
        	
            int testId = Bytes.toInt(key.getBytes(), Bytes.SIZEOF_INT*2 , Bytes.SIZEOF_INT);
            
            List<Long> startTimes = new ArrayList<Long>();
            
            for (LongWritable val : values) {                
            	startTimes.add(val.get());	            
            }
            
            if (startTimes.size() > 1)
            {
            	System.out.println("Multiple test runs for test " + testId + ", serial " + Bytes.head(key.getBytes(), Bytes.SIZEOF_INT));
            	context.getCounter("camstar","multiple runs").increment(1l);
            	put.add(Bytes.toBytes("details"), Bytes.toBytes('*'), new byte[]{1});
            }
            
            Collections.sort(startTimes);
            
            byte[] startTimeBytes = {};
            
            for (long startTime : startTimes)
            {
            	startTimeBytes = Bytes.add(startTimeBytes, Bytes.toBytes(startTime));
            }
            
            put.add(Bytes.toBytes("details"), Bytes.toBytes(testId), startTimeBytes);
            
            context.write(null, put);
        }
    }
	
   
    
    
	/**
	 * @param args
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException, ParseException {
		
		if (args.length > 0)
			new SerialNumberRef(args[0], args[1]).execute();
		else
			new SerialNumberRef(null, null).execute();		
	}
    
    public SerialNumberRef(String startDate, String endDate) throws ParseException
	{
		super(startDate, endDate);
	}

	@Override
	protected void setScanFilter(Scan scan) {
		scan.addFamily(Bytes.toBytes("h"));
	}

	@Override
	protected Class<? extends TableMapper> getMapperClass() {
		return Mapper1.class;
	}

	@Override
	protected Class<? extends Reducer> getCombinerClass() {
		return Combiner.class;
	}

	@Override
	protected Class<? extends TableReducer> getReducerClass() {
		return Reducer1.class;
	}

	@Override
	protected Class<? extends TableMapper> getAggregationMapperClass() { 
		return null;
	}

	@Override
	protected Class<? extends TableReducer> getAggregationReducerClass() {
		return Reducer1.class;
	}

	@Override
	protected Class getMapOutputKeyClass() {
		return BytesWritable.class;
		//return Text.class;
	}

	@Override
	protected Class getMapOutputValueClass() {
		return LongWritable.class;
	}


	@Override
	protected String getResultTableName() {
		return "serial_number_ref";
	}

	@Override
	protected String getInputTableName() {
		return "test_skinny_main";
	}

	
	
}
