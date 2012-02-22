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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;

public class Query6Skinny extends QueryBase {
	
	public static class Mapper1 extends TableMapper<Text, IntWritable> {
      
        private final IntWritable ONE = new IntWritable(1);
        private Text text = new Text();
        
        public Mapper1(){
        	
        }
        
        @Override
        public void map(ImmutableBytesWritable row, Result value, Context context) throws IOException {
        	
        	NavigableMap<byte[], byte[]> family = value.getFamilyMap(Bytes.toBytes("h"));
        	
        	try {
        		//context.write(text, ONE);
        		for (byte[] columnNameBytes : family.keySet())
	        	{	        		        		
        			String columnName = Bytes.toString(columnNameBytes);
        			
	        		if (columnName.endsWith("-gr"))
	        		{	        			
	        				        			
	        			String productId = new String(value.getValue(Bytes.toBytes("h"), Bytes.toBytes("pid")));
	                	String testId = new String(value.getValue(Bytes.toBytes("h"), Bytes.toBytes("tid")));
	                	
	        			String grade = Bytes.toString(family.get(columnNameBytes));        			
	        			String measureName = columnName.substring(0, columnName.indexOf("-gr"));
	        			
	        			String saltString = "";
	        			if (context.getConfiguration().getBoolean("aggregate_by_salt",false))
	        					saltString = "," + row.get()[0];
	        			
	        			text.set(productId + "-" + testId + "-" + measureName  + "-" + grade + saltString );
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

        	int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }

            Put put = new Put(Bytes.toBytes(key.toString()));
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
  
        	try {
        	        
	        	text.set(Arrays.copyOfRange(row.get(), 1, row.getLength()-1));
	        	context.write(text, new IntWritable(Bytes.toInt(value.getValue(Bytes.toBytes("details"), Bytes.toBytes("total")))));
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
	public static void main(String[] args) throws IOException {
		
		new Query6Skinny().execute();

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
		return Text.class;
	}

	@Override
	protected Class getMapOutputValueClass() {
		return IntWritable.class;
	}


	@Override
	protected String getResultTableName() {
		return "query6_skinny_result";
	}

	@Override
	protected String getInputTableName() {
		return "test_skinny";
	}
	
}
