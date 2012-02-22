import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

public class QueryResultScanner {

    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(conf, args[0]);
        
        
        
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        GregorianCalendar cal = new GregorianCalendar();
        
        System.out.println("Setting time...");
        
        //for (byte b = 0; b < 7; b++)
        //{
        	Scan scan = new Scan();
        	cal.setTime(df.parse("2011-01-01 00:00:00"));
        	//scan.setStartRow(Bytes.add(new byte[]{b}, Bytes.add(Bytes.toBytes('D'),Bytes.add(new byte[]{0}, Bytes.toBytes(cal.getTimeInMillis())))));
        	//scan.setStartRow(Bytes.add(new byte[]{(byte)0}, Bytes.add(Bytes.toBytes('D'), new byte[]{0})));
        	cal.setTime(df.parse("2011-02-01 00:00:00"));
        	//scan.setStopRow(Bytes.add(new byte[]{b}, Bytes.add(Bytes.toBytes('D'),Bytes.add(new byte[]{0}, Bytes.toBytes(cal.getTimeInMillis())))));
        	//scan.setStopRow(Bytes.add(new byte[]{(byte)0}, Bytes.add(Bytes.toBytes('D'), new byte[]{1})));
        	
        	ResultScanner scanner = htable.getScanner(scan);
        	
        	
	        Result r;
	        while (((r = scanner.next()) != null)) {
	            /*ImmutableBytesWritable b = r.getBytes();
	            byte[] key = r.getRow();
	            String keyStr = Bytes.toString(key);
	            byte[] totalValue = r.getValue(Bytes.toBytes("details"), Bytes.toBytes("total"));
	            int count = Bytes.toInt(totalValue);
	
	            System.out.println(keyStr+ "," + count);
	            */
	            
	        	
	        	long startTime = Bytes.toLong(r.getRow(), Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT + Bytes.SIZEOF_BYTE, Bytes.SIZEOF_LONG);
	        	char mode = (char) Bytes.toInt(r.getRow(), Bytes.SIZEOF_BYTE, Bytes.SIZEOF_INT);
	        	byte parentByte = r.getRow()[Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT];
	        	cal.setTimeInMillis(startTime);
	        	System.out.println("Salt " + r.getRow()[0] + " mode " + mode + " parentByte " +  parentByte + " Starttime is " + df.format(cal.getTime()));
	        	
	        	
	        	
	        	
	       }
	        scanner.close();
        //}
        
        /*
        Get get = new Get(Bytes.toBytes(54619));
        
        Result result = htable.get(get);
        
        NavigableMap<byte[], byte[]> family = result.getFamilyMap(Bytes.toBytes("name"));
        for (byte[] columnNameBytes : family.keySet())
    	{
        	String columnName = Bytes.toString(columnNameBytes);
        	String value = Bytes.toString(family.get(columnNameBytes));
        	System.out.println("Family: name, Column: " + columnName + ", Value: " + value);
    	}
        
        family = result.getFamilyMap(Bytes.toBytes("id"));
        for (byte[] columnNameBytes : family.keySet())
    	{
        	String columnName = Bytes.toString(columnNameBytes);
        	String value = Bytes.toString(family.get(columnNameBytes));
        	System.out.println("Family: id, Column: " + columnName + ", Value: " + value);
    	}
       
        
        
        Put put = new Put(Bytes.toBytes(14393810));
		put.add(Bytes.toBytes("name"), Bytes.toBytes("s"), Bytes.toBytes("CN0NU2094863016202D7A01"));
		put.setWriteToWAL(false);
		htable.put(put);

		put = new Put(Bytes.toBytes("CN0NU2094863016202D7A01"));
		put.add(Bytes.toBytes("id"), Bytes.toBytes("s"), Bytes.toBytes(14393810));
		put.setWriteToWAL(false);
		htable.put(put);
		
		htable.flushCommits();
		 */
        htable.close();
    }
}
