import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;


public class TestScanner {
	private static Map<String, Map<String, Integer>> uidMap = new HashMap<String, Map<String, Integer>>();
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		try {
			
			
			long startTime = System.currentTimeMillis();
			Configuration conf = HBaseConfiguration.create();
			
			Scan scan = new Scan();
			HTable table = new HTable(conf, "test_uid");
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false); 
			
			scan.setStartRow(Bytes.toBytes(0));
			scan.setStopRow(Bytes.toBytes(Integer.MAX_VALUE));
			scan.addFamily(Bytes.toBytes("name"));
			
			ResultScanner scanner = table.getScanner(scan);
			
			int num = 1;
			
		    for (Result result : scanner) {
		    	
		    	NavigableMap<byte[], byte[]> colummns = result.getFamilyMap(Bytes.toBytes("name"));
		    	for (byte[] columnBytes : colummns.keySet())
	        	{
	        		String column = Bytes.toString(columnBytes);
	        		
	        		Map<String, Integer> columnValueMap = uidMap.get(column);
	        		if (columnValueMap == null)
	        		{
	        			columnValueMap = new HashMap<String, Integer>();
	        			uidMap.put(column, columnValueMap);
	        		}
	        
	        		columnValueMap.put(Bytes.toString(result.getValue(Bytes.toBytes("name"), columnBytes)), Bytes.toInt(result.getRow()));
	        		num++;
	        		
	        		if (num %50000 == 0)
	        			System.out.println("Loaded " + num);
	        	}
		    }
		    
		    System.out.println("Finished, time was " + ((System.currentTimeMillis()-startTime)/1000));
			
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
			throw new RuntimeException (e1);
		}

	}

}
