import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class Query6FatIdScan {

    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        
        HTable htable = new HTable(conf, "test_fat_id_no_salt");
        
        Scan scan = new Scan();
        scan.setCaching(Integer.parseInt(args[2]));
        
        GregorianCalendar cal = new GregorianCalendar();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		
		Date date = df.parse(args[0]);
		cal.setTime(date);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		byte[] startBytes = Bytes.add(Bytes.toBytes('P'), Bytes.toBytes(cal.getTimeInMillis())); 		
		scan.setStartRow(startBytes);				
		/*for (byte i : startBytes)
    	{
    		System.out.print(i + " ");
    	}
		System.out.println();*/
		
		date = df.parse(args[1]);
		cal.setTime(date);
		cal.set(Calendar.HOUR, 23);
		cal.set(Calendar.MINUTE, 59);
		cal.set(Calendar.SECOND, 59);		
		byte[] stopBytes = Bytes.add(Bytes.toBytes('P'), Bytes.toBytes(cal.getTimeInMillis()));
		/*for (byte i : stopBytes)
    	{
    		System.out.print(i + " ");
    	}
		System.out.println();*/
		scan.setStopRow(stopBytes);
    
        scan.addFamily(Bytes.toBytes("h"));
        
        
        long startTime = System.currentTimeMillis();
        ResultScanner scanner = htable.getScanner(scan);
        Result value;
        
        HashMap<byte[], Integer> distinctValues = new HashMap<byte[], Integer>();
        int unitReportCount = 0;
        while (((value = scanner.next()) != null)) {
        	unitReportCount++;        
        	
        	NavigableMap<byte[], byte[]> family = value.getFamilyMap(Bytes.toBytes("h"));
        	
        	try {

        		for (byte[] columnNameBytes : family.keySet())
	        	{	        		        		
        			if (columnNameBytes.length == Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT + Bytes.SIZEOF_INT)
        			{
        			
	        			char columnType = (char) Bytes.toInt(columnNameBytes, Bytes.SIZEOF_LONG+Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);	        			
	        			if (columnType == 'g')
	        			{
	        				// We have a measure grade        			
	        				        			
	        				byte[] colPrefix = Arrays.copyOfRange(columnNameBytes, 0, Bytes.SIZEOF_LONG);
		        			
		        			int productId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.toBytes('p')));
		                	int testId = Bytes.toInt(value.getValue(Bytes.toBytes("h"), Bytes.add(colPrefix,Bytes.toBytes('t'))));
		                	
		        			int gradeId = Bytes.toInt(family.get(columnNameBytes));        			
		        			int measureId = Bytes.toInt(columnNameBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
		        			        					        					        			
		        			byte[] bytes = Bytes.add(Bytes.toBytes(productId), Bytes.add(Bytes.toBytes(testId), Bytes.add(Bytes.toBytes(gradeId), Bytes.toBytes(measureId))));
		        			
		        			Integer count = 1;
		        			if (distinctValues.keySet().contains(bytes))
		        				count = distinctValues.get(bytes);
		        			distinctValues.put(bytes, ++count);
		        			
		        		}
        			}
	        	}
	        	
        		
        	} catch (Exception e) {
        		e.printStackTrace();
                throw new RuntimeException(e);
            }
                           
        }
        
        scanner.close();               
        htable.close();
        
        long endTime = System.currentTimeMillis();
        
        for (byte[] bytes : distinctValues.keySet())
        {
        	System.out.println("bytes: " + bytes + "\t" + distinctValues.get(bytes));
        }
        System.out.println("Processed " + unitReportCount + " unit reports.");
        System.out.println("Ran in " + (endTime-startTime)/1000 + " s (" + (endTime-startTime) + " ms)");
    }
}
