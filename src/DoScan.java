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

public class DoScan {

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
			
		
		
			Scan scan = new Scan();
			HTable table = new HTable(conf, "test");
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false);  // don't set to true for MR jobs
			
			scan.setStartRow(Bytes.add(Bytes.toBytes(0), Bytes.toBytes("-2011-01-01-P")));			
			scan.setStopRow(Bytes.add(Bytes.toBytes(0), Bytes.toBytes("-2011-01-31-P")));
		
			scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));
			scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("TestID"));
			scan.addFamily(Bytes.toBytes("m"));
			
			List<Job> jobList = new ArrayList<Job>();	
		
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
			
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}

	
}
