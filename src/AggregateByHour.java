import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
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
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
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

public class AggregateByHour extends QueryBase {
	
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
        	
        	
        	NavigableMap<byte[], byte[]> family = value.getFamilyMap(Bytes.toBytes("h"));
        	
        	GregorianCalendar cal = new GregorianCalendar();
        	cal.setTimeInMillis(Bytes.toLong(row.get(), Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_LONG));
        	
        	//cal.set(Calendar.HOUR, 0);
        	cal.set(Calendar.MINUTE, 0);
        	cal.set(Calendar.SECOND, 0);
        	
        	byte isParent = row.get()[Bytes.SIZEOF_INT];
        	
        	int testId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('t')));
        	int productId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('p')));
        	int stationId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('l')));
        	int operatorId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('o')));
        	int serialNumberId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('s')));
        	
        	Get get = new Get(Bytes.toBytes(serialNumberId));
        	get.setFilter(new RowFilter());
        	
        	byte[] aggId = Bytes.add(Bytes.toBytes(cal.getTimeInMillis()), Bytes.add(new byte[] {isParent}, Bytes.add(Bytes.toBytes(testId), Bytes.add(Bytes.toBytes(productId), Bytes.add(Bytes.toBytes(stationId), Bytes.toBytes(operatorId))))));
        	
        	// Emits!        	
			int gradeId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('g')));
			try {
		
				context.write(new BytesWritable(Bytes.add(aggId, Bytes.add(Bytes.toBytes('g'), Bytes.toBytes(gradeId)))), ONE); // PASS and FAIL count
		
				
        		//context.write(text, ONE);
        		for (byte[] columnNameBytes : family.keySet())
	        	{	        		        		
        			if (columnNameBytes.length == Bytes.SIZEOF_INT + Bytes.SIZEOF_INT)
        			{
        			
	        			char columnType = (char) Bytes.toInt(columnNameBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
	        			
	        			if (columnType == 'g')
	        			{
	        				// We have a measure grade        			
	        				        				        				
		        			
		        			   			
		        			
		        			int measureGradeId = Bytes.toInt(family.get(columnNameBytes));        			
		        			int measureId = Bytes.toInt(columnNameBytes, 0, Bytes.SIZEOF_INT);
		        			
		        			//String comboStr = uidMap.get("p").get(productId) + "," + uidMap.get("t").get(testId) + "," + uidMap.get("mn").get(measureId) + "," + uidMap.get("mg").get(gradeId);
		        			//context.getCounter("camstar",comboStr).increment(1l);
		        			context.write(new BytesWritable(Bytes.add(aggId, Bytes.add(Bytes.toBytes('m'), Bytes.add(Bytes.toBytes(measureId), Bytes.toBytes(measureGradeId))))), ONE);
		        			//text.set(comboStr);
		        			//context.write(text, ONE);
		        			
		        		}
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
	    
	
    public static class Reducer1 extends TableReducer<BytesWritable, IntWritable, ImmutableBytesWritable> {

        public void reduce(BytesWritable key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            
            Put put = new Put(Bytes.head(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 4));
            char type = (char) Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 4, Bytes.SIZEOF_INT);
            
            if (type == 'g')
            {
            	int gradeId = Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 5, Bytes.SIZEOF_INT);
            	put.add(Bytes.toBytes("details"), Bytes.add(Bytes.toBytes('g'),Bytes.toBytes(gradeId)), Bytes.toBytes(sum));	
            }
            else if (type == 'm')
            {
            	int measureId = Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 5, Bytes.SIZEOF_INT);
            	int measureGradeId = Bytes.toInt(key.getBytes(), Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT * 6, Bytes.SIZEOF_INT);
            	put.add(Bytes.toBytes("details"), Bytes.add(Bytes.toBytes('m'), Bytes.add(Bytes.toBytes(measureId), Bytes.toBytes(measureGradeId))), Bytes.toBytes(sum));	
            }
                        
            context.write(null, put);
        }
    }
	
   
    
    
	/**
	 * @param args
	 * @throws IOException 
	 */
    public static void main(String[] args) throws IOException, ParseException {
		
		if (args.length > 0)
			new AggregateByHour(args[0], args[1]).execute();
		else
			new AggregateByHour(null, null).execute();		
	}
    
    public AggregateByHour(String startDate, String endDate) throws ParseException
	{
		super(startDate, endDate);
	}

    private static final Charset CHARSET = Charset.forName("ISO-8859-1");
    
	@Override
	protected void setScanFilter(Scan scan) {
		scan.addFamily(Bytes.toBytes("h"));
		/*SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		GregorianCalendar calStart = new GregorianCalendar();	
		GregorianCalendar calStop = new GregorianCalendar();
		try {
			calStart.setTime(df.parse("2011-05-27 00:00:00"));			
	    	scan.setStartRow(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{1},Bytes.toBytes(calStart.getTimeInMillis()))));
	    	calStop.setTime(df.parse("2011-06-11 00:00:00"));
	    	scan.setStopRow(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{1},Bytes.toBytes(calStop.getTimeInMillis()))));
	    	*/
	    	/*String patternString = "";
			int i = 0;
			
			//System.out.println("size = " +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().length());
			
			//System.out.println("long is " + cal.getTimeInMillis());
			
			for (i = 0; i < CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStart.getTimeInMillis()))).toString().length(); i++)
			{
				if (CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStart.getTimeInMillis()))).toString().charAt(i) == CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStop.getTimeInMillis()))).toString().charAt(i))
				{
					patternString += CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStart.getTimeInMillis()))).toString().charAt(i) + "{1}";
					//System.out.println("CHAR: " + (int) CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(i));
					//System.out.println(patternString);
				}
				else {
					break;//System.out.println("NO CHAR!!!!!!!!!!");
				}
			}
			
			patternString += "[" +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStart.getTimeInMillis()))).toString().charAt(i) + "-" +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(calStop.getTimeInMillis()))).toString().charAt(i) + "]{1}";
			
			System.out.println("Pattern = " + patternString);
			
			String keyRegex = "^.{" + Bytes.SIZEOF_INT + "}.{" + Bytes.SIZEOF_BYTE + "}" + patternString;

			//keyRegex += ".{" + Bytes.SIZEOF_LONG + "}" ;
	    	
	    	scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(keyRegex)));
	    	*/
	    	
		/*} catch (Exception e) { 
			e.printStackTrace();
			throw new RuntimeException(e);
		}*/
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
		return IntWritable.class;
	}


	@Override
	protected String getResultTableName() {
		return "aggregate_by_hour";
	}

	@Override
	protected String getInputTableName() {
		return "test_skinny_main";
	}

	
	
}
