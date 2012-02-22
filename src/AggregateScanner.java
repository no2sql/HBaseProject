import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class AggregateScanner {

	
	private static  HBaseConfiguration conf = new HBaseConfiguration();
	
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
    
	
    public static void main(String[] args) throws Exception {

        HTable htable = new HTable(conf, "aggregate");
        
        
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
        GregorianCalendar cal = new GregorianCalendar();
                 
    	Scan scan = new Scan();
    	scan.setCaching(1000);
    	//cal.setTime(df.parse("2011-06-10 00:00:00"));
    	//scan.setStartRow(Bytes.toBytes(cal.getTimeInMillis()));
    	//scan.setStartRow(Bytes.add(new byte[]{(byte)0}, Bytes.add(Bytes.toBytes('D'), new byte[]{0})));
    	//cal.setTime(df.parse("2011-06-11 00:00:00"));
    	//scan.setStopRow(Bytes.toBytes(cal.getTimeInMillis()));
    	//scan.setStopRow(Bytes.add(new byte[]{(byte)0}, Bytes.add(Bytes.toBytes('D'), new byte[]{1})));
    	
    	String keyRegex = "^.{" + Bytes.SIZEOF_LONG + "}" /* day */+ CHARSET.decode(ByteBuffer.wrap(new byte[]{(byte)1})).toString() + "{1}" /* parent or not */;
    	keyRegex += ".{" + Bytes.SIZEOF_INT + "}"; // test ID
    	keyRegex += ".{" + Bytes.SIZEOF_INT + "}"; // product ID
    	keyRegex += ".{" + Bytes.SIZEOF_INT + "}"; // station ID
    	//keyRegex += Bytes.toString(Bytes.toBytes(2)) + "{1}"; // operator ID
    	keyRegex += ".{" + Bytes.SIZEOF_INT + "}";  // operator ID
    	scan.setFilter(new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(keyRegex)));
    	
    	long start = System.currentTimeMillis();
    	ResultScanner scanner = htable.getScanner(scan);
    	
    	Map<String, Long> resultMap = new HashMap<String, Long>();
    	
        Result r;
        while (((r = scanner.next()) != null)) {
           
        	ImmutableBytesWritable b = r.getBytes();
            byte[] key = r.getRow();
            
            // Key is startTime(as long)isParent(1 for yes, 0 for no as byte)testId(int)productId(int)stationId(int)operatorId(int)
            long startTime = Bytes.toLong(key, 0, Bytes.SIZEOF_LONG);
            cal.setTimeInMillis(startTime);
            
            byte isParent = key[Bytes.SIZEOF_LONG];
            
            int testId = Bytes.toInt(key, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_INT);
            String test = uidMap.get("t").get(testId);
            
            int productId = Bytes.toInt(key, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            String product = uidMap.get("p").get(productId);
            
            int stationId = Bytes.toInt(key, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT*2, Bytes.SIZEOF_INT);
            String station = uidMap.get("l").get(stationId);
            
            int operatorId = Bytes.toInt(key, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT*3, Bytes.SIZEOF_INT);
            String operator = uidMap.get("o").get(operatorId);
            
            
            
            NavigableMap<byte[], byte[]> family = r.getFamilyMap(Bytes.toBytes("details"));
            for (byte[] columnNameBytes : family.keySet())
        	{
            	char gradeType = (char) Bytes.toInt(columnNameBytes, 0, Bytes.SIZEOF_INT);
            	
            	String aggregateByString = null;
            	
            	if (gradeType == 'g')
            	{
            		
            		// Test count query
            		int gradeId = Bytes.toInt(columnNameBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            		aggregateByString = df.format(cal.getTime()) + " TEST";
            		//aggregateByString = String.valueOf(stationId);
            		Long sum = resultMap.get(aggregateByString);
            		if (sum == null)
            			sum = 0l;
            		resultMap.put(aggregateByString, sum + Bytes.toInt(family.get(columnNameBytes)));
            		
            		
            	}
            	else if (gradeType == 'm')
            	{
            		// Amin query
            		/*int measureId = Bytes.toInt(columnNameBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            		int measureGradeId = Bytes.toInt(columnNameBytes, Bytes.SIZEOF_INT*2, Bytes.SIZEOF_INT);
            		aggregateByString = test + "," + product + "," + uidMap.get("mn").get(measureId) + "," + uidMap.get("mg").get(measureGradeId);
            		
            		Integer sum = resultMap.get(aggregateByString);
            		if (sum == null)
            			sum = 0;
            		resultMap.put(aggregateByString, sum + Bytes.toInt(family.get(columnNameBytes)));
            		*/
            	}
            	else if (/*gradeType == 'l' || */gradeType == 'f')
            	{
            		// Test FPY/LPY count query
            		/*int gradeId = Bytes.toInt(columnNameBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
            		aggregateByString = testId + " " + productId + " " + String.valueOf(gradeId).toUpperCase();
            		//aggregateByString = String.valueOf(stationId);
            		Long sum = resultMap.get(aggregateByString);
            		if (sum == null)
            			sum = 0l;
            		resultMap.put(aggregateByString, sum + Bytes.toInt(family.get(columnNameBytes)));
            		*/
            	}
            	else
            	{
            		//throw new RuntimeException ("Unknown type found " + gradeType);
            	}   
            	        	            
        	}
            
        	
        	
        	//System.out.println("Salt " + r.getRow()[0] + " mode " + mode + " parentByte " +  parentByte + " Starttime is " + df.format(cal.getTime()));
        	
        	
        	
        	 
        }
        
        scanner.close();        
        htable.close();
        HConnectionManager.deleteConnection(conf,true);
        long end = System.currentTimeMillis();
       
        List<String> aggKeyList = new ArrayList<String>();
        aggKeyList.addAll(resultMap.keySet());
        Collections.sort(aggKeyList);
        
        for (String aggKey : aggKeyList)
        {
        	System.out.println(aggKey + "\t" + resultMap.get(aggKey));
        }
        
        System.out.println("Scan took " + (end-start)/1000 + "s");
    }
    
    
    private static Map<String, Map<Integer, String>> uidMap = new HashMap<String, Map<Integer, String>>();
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
		    scanner.close();
		    table.close();
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	}
}
