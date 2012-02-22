import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.HTableDescriptor;
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

public class TableSaveRegionSplits {

    public static void main(String[] args) throws Exception {

        HBaseConfiguration conf = new HBaseConfiguration();
        //HTable htable = new HTable(conf, "test_skinny_id_bigger");
        
        String action = args[0];
        String tableName = args[1];
        
        
          
        if ("save".equals(action))
        {
	        HTable htable = new HTable(conf, tableName);
	
	        byte[][] starts = htable.getStartKeys();
	        List<String> lines = new ArrayList<String>();
	        
	        for (int i = 0; i < starts.length; i++)
	        {
	        	byte[] start = starts[i];        	
	        	System.out.println(Bytes.toStringBinary(start));
	        	String line = "";
	        	for (byte b : start)	
	        	{
	        		line += b + " ";
	        	}		
	        	line = line.trim();
	        	if (line.length() > 0)        		
	        		lines.add(line);	
	        	
	        }
	        
	        FileUtils.writeLines(new File("./" + tableName + "_splits.txt"),lines);
        }
        else if ("create".equals(action))
        {
        	
        	String newTableName = args[2];
        	
        	List<String> lines = FileUtils.readLines(new File("./" + tableName + "_splits.txt"));
        	byte[][] splits = new byte[lines.size()][];
        	
        	for (int i = 0; i < lines.size(); i++)
        	{
        		String line = lines.get(i);
        		line = line.trim();
        		if (line.length() > 0)
        		{
	        		String[] bytes = StringUtils.split(line, " "); 
	    	    	byte[] splitBytes = new byte[bytes.length];
	    	    	int k = 0;
	    	    	for (int j = 0; j < bytes.length; j++)
	    	    	{
	    	    		splitBytes[j] = Byte.parseByte(bytes[j]);
	    	    	}
	    	    	splits[i] = splitBytes;
        		}
        	}
        	        	
        	HBaseAdmin admin = new HBaseAdmin(conf);
        	HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(newTableName));
 		   
			table.addFamily(new HColumnDescriptor(Bytes.toBytes("h"), 1, "snappy", true, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			//table.addFamily(new HColumnDescriptor(Bytes.toBytes("mr"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			table.addFamily(new HColumnDescriptor(Bytes.toBytes("m"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			table.addFamily(new HColumnDescriptor(Bytes.toBytes("cp"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			table.addFamily(new HColumnDescriptor(Bytes.toBytes("p"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			table.addFamily(new HColumnDescriptor(Bytes.toBytes("c"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
   		  
        	
        	admin.createTable( table, splits );
        }
        else
        {
        	System.out.println("Incorrect args, specify: ");
        	System.out.println("\tsave tablename");
        	System.out.println(" - OR - ");
        	System.out.println("\tcreate tablename newtablename");
        }
    }
}
