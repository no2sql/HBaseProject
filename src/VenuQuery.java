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
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class VenuQuery extends QueryBase {
	
	public static class Mapper1 extends TableMapper</*BytesWritable*/Text, IntWritable> {
      
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
		        		if (!"s.".equals(columnBytes))
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
		
        private final IntWritable ONE = new IntWritable(1);
        private BytesWritable combo = new BytesWritable();
        private Text text = new Text();
        
        public Mapper1(){
        	
        }
        
        public static long mapTime = 0;
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
        	
        	long startTime = System.currentTimeMillis();
        	try {
        	
        		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm");
	        	long startDate = Bytes.toLong(row.get(), Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_LONG);
	        	GregorianCalendar cal = new GregorianCalendar();
	        	cal.setTimeInMillis(startDate);	        	
	        	char mode = (char) Bytes.toInt(row.get(), Bytes.SIZEOF_BYTE, Bytes.SIZEOF_INT);
	        	int gradeId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('g')));
	        	
	        	String comboStr = df.format(cal.getTime()) + "," + mode + "," + uidMap.get("g").get(gradeId);
				//context.getCounter("camstar",comboStr).increment(1l);
				//context.write(combo, ONE);
				text.set(comboStr);
				context.write(text, ONE);
			
        	} catch (InterruptedException e) {
                throw new IOException(e);
            }
        	
        }
        
    }

    public static class Reducer1 extends TableReducer</*BytesWritable*/Text, IntWritable, ImmutableBytesWritable> {

        public void reduce(/*BytesWritable*/Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(key.getBytes());
            put.add(Bytes.toBytes("details"), Bytes.toBytes("total"), Bytes.toBytes(sum));
           
            context.write(null, put);
        }
    }
	
    public static class Combiner extends Reducer</*BytesWritable*/Text, IntWritable, /*BytesWritable*/Text, IntWritable>
    {
    	
		@Override
		public void reduce(/*BytesWritable*/ Text key, Iterable<IntWritable> values, Context context)		
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
                sum += val.get();
            }

            context.write(key, new IntWritable(sum));        			
		}
    	
    }
    
    public static class Mapper2 extends TableMapper</*BytesWritable*/Text, IntWritable> {
        
        private final IntWritable ONE = new IntWritable(1);
        /*private BytesWritable combo = new BytesWritable();*/
        private Text combo = new Text();
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
  
        	try {
        		String rowVal = Bytes.toString(row.get());
        		combo.set(rowVal.substring(0,rowVal.length()));
	        	context.write(combo, new IntWritable(Bytes.toInt(value.getValue(Bytes.toBytes("details"), Bytes.toBytes("total")))));
        	}
        	catch (Exception e)
        	{
        		System.out.println("Error");
        		e.printStackTrace();
        	}
        }
    }
    
    
	/**
	 * @param args
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException, ParseException {
		
		if (args.length > 0)
			new VenuQuery(args[0], args[1]).execute();
		else
			new VenuQuery(null, null).execute();		
	}
    
    public VenuQuery(String startDate, String endDate) throws ParseException
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
		return Mapper2.class;
	}

	@Override
	protected Class<? extends TableReducer> getAggregationReducerClass() {
		return Reducer1.class;
	}

	@Override
	protected Class getMapOutputKeyClass() {
		//return BytesWritable.class;
		return Text.class;
	}

	@Override
	protected Class getMapOutputValueClass() {
		return IntWritable.class;
	}


	@Override
	protected String getResultTableName() {
		return "venu_query_result";
	}

	@Override
	protected String getInputTableName() {
		return "test_skinny_id_big";
	}

	
	
}
