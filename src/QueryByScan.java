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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class QueryByScan {

	public static final int NUMBER_OF_NODES = 4;
	
	 
    
    static Map<String,Integer> map = Collections.synchronizedMap(new HashMap<String,Integer>());
    
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		Configuration conf = HBaseConfiguration.create();
		
		List<Thread> threadList = new ArrayList<Thread>();
		long queryStart = System.currentTimeMillis();
		try {
			// Criteria
			String startTime = args[0];//2011-01-28T11:23:20";
			String endTime = args[1];//"2011-01-30T11:23:200";
			String mode = "P";
			
			List<Job> jobList = new ArrayList<Job>();
			for (byte salt =  (NUMBER_OF_NODES-1) * -1; salt < NUMBER_OF_NODES; salt++)
			{		
				Scan scan = new Scan();
				HTable table = new HTable(conf, "test");
				scan.setCaching(5000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
				scan.setCacheBlocks(false);  // don't set to true for MR jobs
				
				scan.setStartRow(Bytes.add(Bytes.toBytes(salt), Bytes.toBytes("-" + startTime + "-" + mode)));
				scan.setStopRow(Bytes.add(Bytes.toBytes(salt), Bytes.toBytes("-" + endTime + "-" + mode)));
			
				scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));
				scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("TestID"));
				scan.addFamily(Bytes.toBytes("m"));
				
				Thread t = new Thread(new ScanThread(table, scan));
				
				threadList.add(t);
				
				t.start();
								
			}
			
			
			for (Thread t : threadList)
			{
				t.join();
			}
			
			System.out.println("RESULTS");
			System.out.println("===========================================");
			
			for (String groupKey: map.keySet())
			{
				System.out.println(groupKey +": " + map.get(groupKey));
			}
			
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
			
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}

	private static long numRecsScanned = 0;
	
	public static class ScanThread implements Runnable
	{
		private HTable table = null;
		private Scan scan = null;
		public ScanThread(HTable table, Scan scan)
		{
			this.table = table;
			this.scan = scan;
		}
		@Override
		public void run() {
			try {
			
				ResultScanner scanner = table.getScanner(scan);
			    for (Result result : scanner) {
			    	
			    	String rowkey = Bytes.toString(result.getRow());
		        	String productId = new String(result.getValue(Bytes.toBytes("h"), Bytes.toBytes("ProductID")));
		        	String testId = new String(result.getValue(Bytes.toBytes("h"), Bytes.toBytes("TestID")));
		        	NavigableMap<byte[], byte[]> measureFamily = result.getFamilyMap(Bytes.toBytes("m"));
		        	
		        	//System.out.println("productId is " + productId);
		        	//System.out.println("testId is " + testId);
	      
		        	for (byte[] columnNameBytes : measureFamily.keySet())
		        	{
		        		String columnName = Bytes.toString(columnNameBytes);
		        		if (columnName.endsWith("-Grade"))
		        		{
		        			String grade = Bytes.toString(measureFamily.get(columnNameBytes));        			
		        			String measureName = columnName.substring(0, columnName.indexOf("-Grade"));
		        			//System.out.println("measureName is " + measureName);
		                	//System.out.println("grade is " + grade);	                	
		        			synchronized(map)
		        			{
			        			String groupKey = productId + "-" + testId + "-" + measureName  + "-" + grade; 
		        				Integer count = map.get(groupKey);
		        				if (count == null)
		        					map.put(groupKey,1);
		        				else
		        					map.put(groupKey, count+1);			        				
		        			}
		        		}
		        	}        	
		        	
		        	numRecsScanned++;
		        	if (numRecsScanned %100000 == 0)
		        		System.out.println("Scanned records " + numRecsScanned);
			    }
			    scanner.close();
			}
			catch (Exception e)
			{
				System.out.println("Error: " + e.getMessage());
				e.printStackTrace();
			}			
			
		}
		
	}
	
}
