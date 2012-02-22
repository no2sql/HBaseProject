import net.sigmaquest.supplychain.model.TestRun;
import net.sigmaquest.supplychain.model.UnitReport;
import net.sigmaquest.supplychain.model.ValueBase;
import net.sigmaquest.supplychain.model.ValueBoolean;
import net.sigmaquest.supplychain.model.ValueDouble;
import net.sigmaquest.supplychain.model.ValueInteger;
import net.sigmaquest.supplychain.model.ValueString;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.Writable;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.math.BigInteger;
import java.sql.Blob;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

public class BulkLoaderFatByte extends Configured implements Tool {

	public static final int NUMBER_OF_NODES = 6;

	private static Map<String, Map<String, Integer>> uidMap = new HashMap<String, Map<String, Integer>>();
	
	static {
		try {
			
			Configuration conf = HBaseConfiguration.create();
			
			Scan scan = new Scan();
			HTable table = new HTable(conf, "test_uid");
			scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
			scan.setCacheBlocks(false); 
			
			scan.setStartRow(Bytes.toBytes(0));
			scan.setStopRow(Bytes.toBytes(Integer.MAX_VALUE));
			scan.addFamily(Bytes.toBytes("name"));
			
			ResultScanner scanner = table.getScanner(scan);
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
	        	}
		    }
			
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
			throw new RuntimeException (e1);
		}
	}
	
	static class BulkLoaderByFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>  {
		
		@Override
		public void map(LongWritable row, Text value, Context context) throws IOException {

		
			
			try {
				List<Put> puts = dbRowToPuts(value);
				for (Put put : puts)
					context.write(new ImmutableBytesWritable(put.getRow()), put);    	
			} catch (InterruptedException e) {
				throw new IOException(e);
			}

		}


	}

	static private JAXBContext jc;
	static private Unmarshaller u;

	public static List<Put> dbRowToPuts(Text value)
	{
		try {
			Unmarshaller u = jc.createUnmarshaller();

			List<Put> puts = new ArrayList<Put>();


			String singleXml = value.toString();

			if (singleXml.indexOf("urn:unitreport-schema") == -1)
			{

				String sub = new String("xmlns");
				int index = singleXml.indexOf(sub);
				String f1 = singleXml.substring(0,index);
				String f2 = singleXml.substring(index);
				//String newxml = f1.concat(" xmlns=\"urn:unitreport-schema\" ").concat(f2);
				singleXml = f1 + " xmlns=\"urn:unitreport-schema\" " + f2;
			}

			if (singleXml.indexOf("</UnitReportList") != -1)
			{												
				singleXml = singleXml.substring(0, singleXml.indexOf("</UnitReportList"));
			}

			UnitReport unitReport = (UnitReport) u.unmarshal(new StringReader(singleXml));
			unitReport.setFilename("??????????");


			putTest(puts, unitReport, unitReport.getTestRun(),null, null);

			//System.out.println("Loaded test " + count++);
			
			
			return puts;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException (e);
		}

	}




	private static Long testRunId = 0l;

	public static Long putTest(List<Put> putList, UnitReport unitReport, TestRun testRun, TestRun parentTestRun, Put put) throws IOException
	{				
		try {
			synchronized (testRunId)
			{
				testRun.setTestId(++testRunId);				
			}
			//System.out.println(testRunId);
			
			if (put == null)
			{
				SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
				String startTime = testRun.getStartTime();
				if (startTime.indexOf(".") > -1)
					startTime = startTime.substring(0,startTime.indexOf(".")) + startTime.substring(startTime.indexOf(".")+4, startTime.length());
				startTime = startTime.substring(0,19) + " " + startTime.substring(19,22) + startTime.substring(23,25);
				Date date = df.parse(startTime);
				GregorianCalendar cal = new GregorianCalendar();
				cal.setTime(date);
				byte prefix = (byte) Math.abs((unitReport.getStartTime().hashCode() % NUMBER_OF_NODES));
				
				Random randomGenerator = new Random();
				
				byte[] rowkey = 
						Bytes.add(
								new byte[] {prefix},
								Bytes.add(																		
											Bytes.toBytes(unitReport.getMode().charAt(0)),
											Bytes.add(
													Bytes.toBytes(cal.getTimeInMillis()),
													Bytes.add(
														Bytes.toBytes(unitReport.getTestRun().getTestId()),
														Bytes.toBytes(randomGenerator.nextInt()
													)
											)
											
										)
								)
								
						);
	
				
				
				put = new Put(rowkey);
			}
			
			byte[] colPrefix = new byte[] {};
			if (parentTestRun != null)
				colPrefix = Bytes.toBytes(testRun.getTestId());
			
			// put column family -header
			if (testRun.getEndTime() != null)
				put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('e')),
						Bytes.toBytes(uidMap.get("e").get(testRun.getEndTime())));
			
			if (unitReport.getTestRun().getGrade() != null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('g')),
					Bytes.toBytes(uidMap.get("g").get(unitReport.getTestRun().getGrade())));
			if (parentTestRun == null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('f')),
					Bytes.toBytes(uidMap.get("f").get(unitReport.getFilename())));
			if (parentTestRun == null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('l')),
					Bytes.toBytes(uidMap.get("l").get(unitReport.getStation().getGuid())));
			if (parentTestRun == null && unitReport.getProduct().getSerialNo() != null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('s')),
					Bytes.toBytes(uidMap.get("s").get(unitReport.getProduct().getSerialNo())));
			if (parentTestRun == null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('p')),
					Bytes.toBytes(uidMap.get("p").get(unitReport.getProduct().getPartNo())));
			if (parentTestRun == null)
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('o')),
					Bytes.toBytes(uidMap.get("o").get(unitReport.getOperator().getName())));
			put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.toBytes('t')),
					Bytes.toBytes(uidMap.get("t").get(testRun.getName())));
			
			// put column family --measurement
			if (testRun.getResult() != null)
			{
				for (Object measure : testRun.getResult().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) measure;
					
					Integer measureId = uidMap.get("mn").get(value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('l'))),
									Bytes.toBytes(uidMap.get("mlsl").get(value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('u'))),
									Bytes.toBytes(uidMap.get("musl").get(value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('n'))),
									Bytes.toBytes(uidMap.get("mu").get(value.getUnitString())));
						if (value.getGradeString() != null)
							put.add(Bytes.toBytes("h"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('g'))),
									Bytes.toBytes(uidMap.get("mg").get(value.getGradeString())));
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.toBytes(measureId)),
									Bytes.toBytes(uidMap.get("mv").get(value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.toBytes(measureId)),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.toBytes(measureId)),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("m"), Bytes.add(colPrefix, Bytes.toBytes(measureId)),
										Bytes.toBytes(((ValueBoolean)value).getValue()));
							}
						}
					}

				}
			}

			// For parent test, include the unit report custom properties as well as that
			// test run's custom properties
			if (parentTestRun == null)
			{
				for (Object property : unitReport.getProperty().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) property;
					
					Integer propId = uidMap.get("pn").get(value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('l'))),
									Bytes.toBytes(uidMap.get("plsl").get(value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('u'))),
									Bytes.toBytes(uidMap.get("pusl").get(value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('n'))),
									Bytes.toBytes(uidMap.get("pu").get(value.getUnitString())));
						
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
									Bytes.toBytes(uidMap.get("pv").get(value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueBoolean)value).getValue()));
							}
						}
					}
				}
			}
			
			
		
			if (testRun.getProperty() != null)
			{
				for (Object property : testRun.getProperty().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) property;
					
					Integer propId = uidMap.get("pn").get(value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('l'))),
									Bytes.toBytes(uidMap.get("plsl").get(value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('u'))),
									Bytes.toBytes(uidMap.get("pusl").get(value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('n'))),
									Bytes.toBytes(uidMap.get("pu").get(value.getUnitString())));
						
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
									Bytes.toBytes(uidMap.get("pv").get(value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("cp"), Bytes.add(colPrefix, Bytes.toBytes(propId)),
										Bytes.toBytes(((ValueBoolean)value).getValue()));
							}
						}
					}
				}
			}
			
			if (testRun.getSubtests() != null)
			{
				for (TestRun subtest: testRun.getSubtests())
				{
					Long subtestId = putTest(putList, unitReport, subtest, testRun, put);					
				}
			}
			

			put.setWriteToWAL(false);
			
			if (parentTestRun == null)
				putList.add(put);		

			return testRun.getTestId();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	}



	static int numComplete = 0;

	@Override
	public int run(String[] args) throws Exception {

		Job job;
		try {
			Configuration conf = getConf();
			//conf.set("mapred.job.tracker", "local");
			//conf.set("fs.default.name", "local");
			
			/*DBConfiguration.configureDB(conf,
				    "oracle.jdbc.driver.OracleDriver",
				    "jdbc:oracle:thin:@10.50.5.29:1521:csnrdee","KFARMER_711","KFARMER");
			*/
			
			//conf.setBoolean("mapred.task.profile", true);
			conf.set("dfs.replication", "1");			
			conf.set(XmlInputFormat.START_TAG_KEY,"<UnitReport");
			conf.set(XmlInputFormat.END_TAG_KEY,"</UnitReport>");
			
			//conf.set("mapred.job.tracker", "local");
			//conf.set("fs.default.name", "local");
			
			job = new Job(conf);
			job.setJobName("Bulk loader by file test_fat_id");			

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);

			job.setMapperClass(BulkLoaderByFileMapper.class);
			job.setJarByClass(BulkLoaderFatByte.class);
			
			job.setInputFormatClass(XmlInputFormat.class);		    
			FileInputFormat.setInputPaths(job, "/xml/input_512");
		
			job.setOutputFormatClass(HFileOutputFormat.class);
			HFileOutputFormat.setOutputPath(job, new Path("/import-data-fat"));
			
			Configuration hConfig = HBaseConfiguration.create(conf);
			createTable(conf);
			conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName());
	        hConfig.setLong("version", System.currentTimeMillis());	        	        
	        
	        
	        HFileOutputFormat.configureIncrementalLoad(job, new HTable(hConfig, "test_fat_id"));
		
			TableMapReduceUtil.addDependencyJars(job);
			/*conf.setInt("mapred.reduce.tasks",4);
			conf.setInt("mapreduce.reduce.tasks",4);
			job.setNumReduceTasks(4);
			*/
			
			
			job.waitForCompletion(true);
			
			return 0;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return -1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		

		long queryStart = System.currentTimeMillis();
		try {
			// Criteria


			ToolRunner.run(new BulkLoaderFatByte(), args);



			// set Mapper, etc., and call JobClient.runJob(conf);




			System.out.println("Load ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}



	public static boolean createTable(Configuration conf)
	throws IOException, ParseException {
	  try {
		  HBaseAdmin admin = new HBaseAdmin(conf);
		  try{
			  admin.disableTable(Bytes.toBytes("test_fat_id"));
			  admin.deleteTable(Bytes.toBytes("test_fat_id"));
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  
		  }
		  
		  HTableDescriptor table = new HTableDescriptor(Bytes.toBytes("test_fat_id"));
		   
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("h"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  //table.addFamily(new HColumnDescriptor(Bytes.toBytes("mr"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("m"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("cp"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  
		  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
		  
		  byte[][] splits = new byte[NUMBER_OF_NODES-2][];
		  int i = 0;
		  for (byte salt = 1; salt < NUMBER_OF_NODES-1; salt++)
		  {		  
			  GregorianCalendar cal = new GregorianCalendar();
			  
			  Date date;
			  splits[i++] = new byte[] {(byte)salt};
			  /*		
			  date = df.parse("2011-02-13T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.add(Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-13T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
			  
			  date = df.parse("2011-05-22T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
			  
			  date = df.parse("2011-05-27T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
			  
			  date = df.parse("2011-06-05T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
			  
			  date = df.parse("2011-06-13T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));			  
			 */
		  }		  
		  admin.createTable( table, splits );
	    return true;
	  } catch (TableExistsException e) {
		  e.printStackTrace();
	    // the table already exists...
	    return false;  
	  }
	}




}
