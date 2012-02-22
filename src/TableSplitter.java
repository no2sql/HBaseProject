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
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class TableSplitter {

    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        //HTable htable = new HTable(conf, "test_skinny_id_bigger");
        
        HBaseAdmin admin = new HBaseAdmin(conf);
               
        HTable htable = new HTable(conf, ".META.");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
        
        
        ResultScanner scanner = htable.getScanner(scan);        
        Result r;
        int i = 1;
        Map<Integer, byte[]> regionMap = new HashMap<Integer, byte[]>();
        
        while (((r = scanner.next()) != null) /*&& i < 10*/) {
            
            
            /*NavigableMap<byte[], byte[]> family = r.getFamilyMap(Bytes.toBytes("h"));
            for (byte[] columnNameBytes : family.keySet())
        	{	        		        		
    			String columnName = Bytes.toString(columnNameBytes);
    			
    			System.out.println("columnName: " + columnName);
        	}*/
        	
        	for (byte b : r.getRow())
        	{
        		System.out.print(b + " " );
        	}
        	System.out.println();
        	System.out.println(i + ": " + Bytes.toStringBinary(r.getRow()));
        	regionMap.put(i, r.getRow());
            i++;
        }
        scanner.close();
        htable.close();
        
        
        
         
        HConnection connection =
                HConnectionManager.getConnection(conf);
              CatalogTracker ct = new CatalogTracker(connection);
              ct.start();
              
         Scanner inputScanner = new Scanner(System.in);
         String input = null;
         System.out.println("Enter region number to split, \"exit\" to exit");
         while (!(input = inputScanner.next()).equalsIgnoreCase("exit")) 
         {
	         Integer region = Integer.parseInt(input);
	         byte[] regionBytes = regionMap.get(region);
        	 Pair<HRegionInfo, HServerAddress> pair =	          
	                      MetaReader.getRegion(ct, regionBytes);
	         System.out.println("Splitting region " + Bytes.toStringBinary(regionBytes));
	         admin.split(regionBytes);
	         System.out.println("Enter region number to split, \"exit\" to exit");	        
         }
              
    }
}
