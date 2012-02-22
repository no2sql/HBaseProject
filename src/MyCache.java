import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/*
import net.spy.memcached.AddrUtil;
import net.spy.memcached.BinaryConnectionFactory;
import net.spy.memcached.MemcachedClient;
*/


public class MyCache {

	//private MemcachedClient c;
	private static MyCache instance = null;
	//private static MemcachedClient[] m = null;

	private static Configuration conf = HBaseConfiguration.create();
	private static Map<String, Map<String, Integer>> uidMap = new HashMap<String, Map<String, Integer>>();
	
	
	private MyCache() {
		try {
			/*m= new MemcachedClient[21];
			for (int i = 0; i <= 20; i ++) {
				MemcachedClient c =  new MemcachedClient(
                                    new BinaryConnectionFactory(),
                                    AddrUtil.getAddresses("10.50.7.11:1121 10.50.7.12:1121 10.50.7.13:1121 10.50.7.14:1121 10.50.7.15:1121 10.50.7.16:1121 10.50.7.17:1121"));
				m[i] = c;
			}*/
						
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
	        		if (!"s.".equals(columnBytes))
	        		{
		        		Map<String, Integer> columnValueMap = uidMap.get(column);
		        		if (columnValueMap == null)
		        		{
		        			columnValueMap = new HashMap<String, Integer>();
		        			uidMap.put(column, columnValueMap);
		        		}
		        
		        		columnValueMap.put(Bytes.toString(result.getValue(Bytes.toBytes("name"), columnBytes)), Bytes.toInt(result.getRow()));
	        		}
	        	}
		    }
			
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	}

	public static synchronized MyCache getInstance() {
		//System.out.println("Instance: " + instance);
		if(instance == null) {
			//System.out.println("Creating a new instance");
			instance = new MyCache();
	     }
	     return instance;
	}
/*
	public void set(String wholeKey, int ttl, final int o) {		
		getCache().set(wholeKey, ttl, o);
	}
*/
	public int get(String type, String key) {
		
		
		if (key == null)
			return -1;
		
			
		if (!"s".equals(type))
		{
			Integer o = uidMap.get(type).get(key);
	        /*if(o == null) {
	        	System.out.println("Cache MISS for KEY: " + key);
	        } else {
	            System.out.println("Cache HIT for KEY: " + key);
	        }*/
	        if (o != null)
	        	return o;
	        else
	        	throw new RuntimeException ("Object ID not found in lookup for type: " + type + ", key: " + key);
		}
        
        try {
        	HTable table = new HTable(conf, "test_uid");
        	Get get = new Get(Bytes.toBytes(key.trim()));
        	get.addColumn(Bytes.toBytes("id"), Bytes.toBytes(type));
        	Result result = table.get(get);
        	if (result.getValue(Bytes.toBytes("id"), Bytes.toBytes(type)) == null)
        		throw new RuntimeException("NULL for type " + type + " key " + key.trim());
        	int val = Bytes.toInt(result.getValue(Bytes.toBytes("id"), Bytes.toBytes(type)));
        	return val;
        }
        catch (Exception e)
        {
        	e.printStackTrace();
        	throw new RuntimeException(e);
        }
	}
/*
	public Object delete(String key) {
		return getCache().delete(key);
	}

	public MemcachedClient getCache() {
		MemcachedClient c= null;
		try {
			int i = (int) (Math.random()* 20);
			c = m[i];
		} catch(Exception e) {

		}
		return c;
	}
	*/
}

	