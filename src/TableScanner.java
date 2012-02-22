import java.text.SimpleDateFormat;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.catalog.CatalogTracker;
import org.apache.hadoop.hbase.catalog.MetaReader;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class TableScanner {

	
	
    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        HTable htable = new HTable(conf, "test_skinny_main");

        Scan scan = new Scan();
        scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes('p'));
                
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        GregorianCalendar cal = new GregorianCalendar();
        cal.setTime(df.parse("2011-06-10 00:00:00"));
        scan.setStartRow(Bytes.add(Bytes.toBytes('P'), Bytes.add(new byte[]{1}, Bytes.toBytes(cal.getTimeInMillis()))));
        cal.setTime(df.parse("2011-06-11 00:00:00"));
        scan.setStopRow(Bytes.add(Bytes.toBytes('P'), Bytes.add(new byte[]{1}, Bytes.toBytes(cal.getTimeInMillis()))));
        
        ResultScanner scanner = htable.getScanner(scan);        
        Result r;
        int i = 0;
        
        while (((r = scanner.next()) != null)) {
            
            i++;
            /*int serialNumberId = Bytes.toInt(r.getRow());
            int productId = Bytes.toInt(r.getValue(Bytes.toBytes("details"), Bytes.toBytes('p')));
            
            NavigableMap <byte[], byte[]> familyMap = r.getFamilyMap(Bytes.toBytes("details"));
            //System.out.println(serialNumberId + " " + productId);
            for (byte[] columnBytes : familyMap.keySet())
            {
            	if (columnBytes.length == Bytes.SIZEOF_INT*2){
            	int testId = (int) Bytes.toInt(columnBytes, 0 , Bytes.SIZEOF_INT);
            	
	            char startChar = (char) Bytes.toInt(columnBytes, Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
	            byte[] valBytes = familyMap.get(columnBytes);
	            long startTime = Bytes.toLong(valBytes, 0, Bytes.SIZEOF_LONG);
	            
	            int gradeId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG, Bytes.SIZEOF_INT);
	            
	            byte isParent = valBytes[Bytes.SIZEOF_LONG + Bytes.SIZEOF_INT];
	            
	            int stationId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT, Bytes.SIZEOF_INT);
	            int operatorId = Bytes.toInt(valBytes, Bytes.SIZEOF_LONG + Bytes.SIZEOF_BYTE + Bytes.SIZEOF_INT*2, Bytes.SIZEOF_INT);
	            cal.setTimeInMillis(startTime);
	            //if (stationId != 1)
	            System.out.println("\t" + testId + " " + startChar + " " + df.format(cal.getTime()) + " " + gradeId + " " + isParent + " " + stationId + " " + operatorId);
	            
            	}
            }*/
        	//break;
            if (i % 100000 == 0)
            	System.out.println(i);
        }
        
        System.out.println(i);
        
        scanner.close();
        htable.close();
        
        
              
    }
}
