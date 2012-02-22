import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.hbase.filter.BinaryPrefixComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class RegionMerger extends Configured implements Tool {

	
	
    @Override
	public int run(String[] arg0) throws Exception {
    	doMerge();
    	return 0;
	}

	public static void main(String[] args) throws Exception {

    	//regionNamesToFile();
        
		ToolRunner.run(new RegionMerger(), args);
		
    }
    
    public void doMerge() throws Exception
    {
    	
    	Configuration conf = getConf();
    	Configuration hConfig = HBaseConfiguration.create(conf);
    	
    	List lines = FileUtils.readLines(new File("./regions_to_merge_bytes.txt"));
    	for (int i = 0; i < lines.size(); i += 2)
    	{
	    	String line = (String) lines.get(i);
	    	line = line.trim();
	    	
	    	if (line.length() == 0)
	    		break;
    		
	    	String[] bytes = StringUtils.split(line, " "); 
	    	byte[] regionBytes = new byte[bytes.length];
	    	int k = 0;
	    	for (String byteStr : bytes)
	    	{
	    		regionBytes[k++] = Byte.parseByte(byteStr);
	    	}
	    	
	    	
	    	line = (String) lines.get(i+1);
	    	line = line.trim();
    		
	    	bytes = StringUtils.split(line, " "); 
	    	byte[] nextRegionBytes = new byte[bytes.length];
	    	k = 0;
	    	for (String byteStr : bytes)
	    	{
	    		nextRegionBytes[k++] = Byte.parseByte(byteStr);
	    	}
	    	
	    	System.out.println("Merging " + Bytes.toStringBinary(regionBytes) + " with " + Bytes.toStringBinary(nextRegionBytes));
	    	
    		Merge merge = new Merge(hConfig);                
	        merge.doMerge(Bytes.toBytes("test_skinny_main"), regionBytes, nextRegionBytes);
    	}
    	System.out.println("Done merging....");
    }
    
    public static void regionNamesToFile() throws IOException
    {
    	HBaseConfiguration conf = new HBaseConfiguration();
        //HTable htable = new HTable(conf, "test_skinny_id_bigger");
        
        HBaseAdmin admin = new HBaseAdmin(conf);
               
        HTable htable = new HTable(conf, ".META.");

        Scan scan = new Scan();
        scan.addFamily(Bytes.toBytes("info"));
                
        Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL,
        		new BinaryPrefixComparator(Bytes.toBytes("test_skinny_main")));
        
        scan.setFilter(filter);

        
        ResultScanner scanner = htable.getScanner(scan);        
        Result r;
        int i = 1;
        //Map<Integer, byte[]> regionMap = new HashMap<Integer, byte[]>();
        List<byte[]> regionList = new ArrayList<byte[]>();
        
        while (((r = scanner.next()) != null) /*&& i < 10*/) {
            /*
        	for (byte b : r.getRow())
        	{
        		System.out.print(b + " " );
        	}
        	System.out.println();
        	*/
       // 	System.out.println(i + ": " + Bytes.toStringBinary(r.getRow()));
        	
        	regionList.add(r.getRow());
            i++;
        }
        scanner.close();
        htable.close();
     //   System.out.println(i);
        
        
        
        List<String> emptyRegions = FileUtils.readLines(new File("./regions_to_merge.txt"));
        List<byte[]> mergingRegions = new ArrayList<byte[]>();
        
        List<String> lines = new ArrayList<String>();
        
        for (int j = 0; j < emptyRegions.size(); j++)
        {
        	String regionPart = emptyRegions.get(j);
        	
        	byte[] region = findRegion(regionList, regionPart); 
        	byte[] nextRegion = findNextRegion(regionList, region);
        	
        	String line = "";
        	if (nextRegion != null)
        	{
		        try{
		        	for (byte b : region)	
		        	{
		        		line += b + " ";
		        	}		
		        	lines.add(line);
		        			
					line = "";
		        	for (byte b : nextRegion)
		        	{
		        		line += b + " ";
		        	}
	        	}
	        	catch (Exception e)
	        	{
	        		e.printStackTrace();
	        		System.out.println("Error finding " + regionPart);
	        		throw new RuntimeException(e);
	        	}
	        	
		        if (!mergingRegions.contains(region))
		        {
		        	lines.add(line);
		        	mergingRegions.add(region);
	        		mergingRegions.add(nextRegion);
		        }
	        }
        	/*if (nextRegion != null && !mergingRegions.contains(region))
        	{
        		//System.out.println("hbase org.apache.hadoop.hbase.util.Merge test_skinny_main "+ region + " " + nextRegion);
        		Merge merge = new Merge();                
                merge.doMerge(Bytes.toBytes("test_skinny_main"), region, nextRegion);
                
        		mergingRegions.add(region);
        		mergingRegions.add(nextRegion);
        	}*/
        }
        
        FileUtils.writeLines(new File("./regions_to_merge_bytes.txt"),lines);
        
        
         /*
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
         }*/
    }
    
    public static byte[] findRegion(List<byte[]> regionList, String regionPart)
    {
    	for (byte[] regionBytes : regionList)
    		if (Bytes.toStringBinary(regionBytes).endsWith(regionPart + "."))
    			//return Bytes.toStringBinary(regionBytes);
    			return regionBytes;    			
    	return null;
    }
    
    public static byte[] findNextRegion(List<byte[]> regionList, byte[] region)
    {    	
    	for (int i = 0; i < regionList.size(); i++)
    	{
    		byte[] regionBytes = regionList.get(i);
    		if (i == regionList.size() -1)
    			return null;
    		if (Arrays.equals(regionBytes, region))
    			//return Bytes.toStringBinary(regionList.get(i+1));
    			return regionList.get(i+1);
    	}		
    	return null;
    }
}
