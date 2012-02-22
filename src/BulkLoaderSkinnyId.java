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

public class BulkLoaderSkinnyId extends Configured implements Tool {

	public static final int NUMBER_OF_NODES = 7;
	public static final int NUMBER_OF_REPLICATIONS = 20;
	public static final int INCREMENT_MINUTES = 30;

	static {
		try {
			jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}
	}
	
	static class BulkLoaderByFileMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>  {
		
		@Override
		public void map(LongWritable row, Text value, Context context) throws IOException {
			
			useSalt = context.getConfiguration().getBoolean("use_salt",true);
		
			try {
				List<Put> puts = dbRowToPuts(value);
				for (Put put : puts)
					context.write(new ImmutableBytesWritable(put.getRow()), put);   
						
				context.getCounter("camstar","Number of puts " + useSalt).increment(puts.size());
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

			Integer serialNoId = MyCache.getInstance().get("s", unitReport.getProduct().getSerialNo());

			for (int i = 0; i < NUMBER_OF_REPLICATIONS; i++)
				putTest(puts, unitReport, unitReport.getTestRun(),null, i*INCREMENT_MINUTES, serialNoId);
			
			

			//System.out.println("Loaded test " + count++);
			
			
			return puts;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException (e);
		}

	}

	private static Long testRunId = 0l;

	public static Long putTest(List<Put> putList, UnitReport unitReport, TestRun testRun, TestRun parentTestRun, int incrementMinutes, Integer serialNoId)throws IOException
	{				
		try {
			
			synchronized (testRunId)
			{
				testRun.setTestId(++testRunId);				
			}
			//System.out.println(testRunId);
			
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
			String startTime = testRun.getStartTime();
			if (startTime.indexOf(".") > -1)
				startTime = startTime.substring(0,startTime.indexOf(".")) + startTime.substring(startTime.indexOf(".")+4, startTime.length());
			startTime = startTime.substring(0,19) + " " + startTime.substring(19,22) + startTime.substring(23,25);
			Date date = df.parse(startTime);
			
			
			Date loadThreshold = df.parse("2011-06-11T00:00:00 +0500");
			if (date.after(loadThreshold))
				return -1l;
			
						
			GregorianCalendar cal = new GregorianCalendar();
			cal.setTime(date);
			cal.add(GregorianCalendar.MINUTE, incrementMinutes);
			
			byte prefix = (byte) Math.abs((df.format(cal.getTime()).hashCode() % NUMBER_OF_NODES));
			
			Random randomGenerator = new Random();
			
			byte[] rowkey = 
					Bytes.add(
						(useSalt ? new byte[] {prefix} : new byte[]{}),
						Bytes.add(																		
									Bytes.toBytes(unitReport.getMode().charAt(0)),
									Bytes.add(												
										new byte[] {(byte)(parentTestRun == null ? 1 : 0)},
										Bytes.add(
											Bytes.toBytes(cal.getTimeInMillis()),
											Bytes.add(
													Bytes.toBytes(unitReport.getTestRun().getTestId()),
													Bytes.toBytes(randomGenerator.nextInt()
											)
										)												
									)											
								)
							)
						);
							
					

			
			
			Put put = new Put(rowkey);
					
			// put column family -header
			if (testRun.getEndTime() != null)
				put.add(Bytes.toBytes("h"), Bytes.toBytes('e'),
						Bytes.toBytes(MyCache.getInstance().get("e", testRun.getEndTime())));
			
			if (testRun.getGrade() != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes('g'),
					Bytes.toBytes(MyCache.getInstance().get("g", testRun.getGrade())));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes('f'),
					Bytes.toBytes(MyCache.getInstance().get("f", unitReport.getFilename())));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes('l'),
					Bytes.toBytes(MyCache.getInstance().get("l", unitReport.getStation().getGuid())));
			if (serialNoId != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes('s'),
					Bytes.toBytes(serialNoId));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes('p'),
					Bytes.toBytes(MyCache.getInstance().get("p", unitReport.getProduct().getPartNo())));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes('o'),
					Bytes.toBytes(MyCache.getInstance().get("o", unitReport.getOperator().getName())));
			put.add(Bytes.toBytes("h"), Bytes.toBytes('t'),
					Bytes.toBytes(MyCache.getInstance().get("t", testRun.getName())));
			
			// put column family --measurement
			if (testRun.getResult() != null)
			{
				for (Object measure : testRun.getResult().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) measure;
					
					Integer measureId = MyCache.getInstance().get("mn", value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('l')),
									Bytes.toBytes(MyCache.getInstance().get("mlsl", value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('u')),
									Bytes.toBytes(MyCache.getInstance().get("musl", value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("m"), Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('n')),
									Bytes.toBytes(MyCache.getInstance().get("mu", value.getUnitString())));
						if (value.getGradeString() != null)
							put.add(Bytes.toBytes("h"), Bytes.add(Bytes.toBytes(measureId),Bytes.toBytes('g')),
									Bytes.toBytes(MyCache.getInstance().get("mg", value.getGradeString())));
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("m"), Bytes.toBytes(measureId),
									Bytes.toBytes(MyCache.getInstance().get("mv", value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("m"), Bytes.toBytes(measureId),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("m"), Bytes.toBytes(measureId),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("m"), Bytes.toBytes(measureId),
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
					
					Integer propId = MyCache.getInstance().get("pn", value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('l')),
									Bytes.toBytes(MyCache.getInstance().get("plsl", value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('u')),
									Bytes.toBytes(MyCache.getInstance().get("pusl", value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('n')),
									Bytes.toBytes(MyCache.getInstance().get("pu", value.getUnitString())));
						
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
									Bytes.toBytes(MyCache.getInstance().get("pv", value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
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
					
					Integer propId = MyCache.getInstance().get("pn", value.getNameString());
					
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('l')),
									Bytes.toBytes(MyCache.getInstance().get("plsl", value.getLslString())));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('u')),
									Bytes.toBytes(MyCache.getInstance().get("pusl", value.getUslString())));						
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("cp"), Bytes.add(Bytes.toBytes(propId),Bytes.toBytes('n')),
									Bytes.toBytes(MyCache.getInstance().get("pu", value.getUnitString())));
						
						if (value.getValueString() != null)
						{
							if (value instanceof ValueString)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
									Bytes.toBytes(MyCache.getInstance().get("pv", value.getValueString())));
							}
							else if (value instanceof ValueInteger)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
										Bytes.toBytes(((ValueInteger)value).getValue().intValue()));
							}
							else if (value instanceof ValueDouble)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
										Bytes.toBytes(((ValueDouble)value).getValue()));
							}
							else if (value instanceof ValueBoolean)
							{
								put.add(Bytes.toBytes("cp"), Bytes.toBytes(propId),
										Bytes.toBytes(((ValueBoolean)value).getValue()));
							}
						}
					}
				}
			}
			
			// put column family --- parent
			if (parentTestRun != null) {
				put.add(Bytes.toBytes("p"), Bytes.toBytes(parentTestRun.getTestId()),
						Bytes.toBytes(MyCache.getInstance().get("g", parentTestRun.getGrade())));
			}
			
			if (testRun.getSubtests() != null)
			{
				for (TestRun subtest: testRun.getSubtests())
				{
					Long subtestId = putTest(putList, unitReport, subtest, testRun, incrementMinutes, serialNoId);	
					put.add(Bytes.toBytes("c"), Bytes.toBytes(subtestId), Bytes.toBytes(MyCache.getInstance().get("g", subtest.getGrade())));
				}
			}
			

			put.setWriteToWAL(false);
			
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



	private static boolean useSalt = false;
	private static String tableName = "test_skinny_main";
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
			
			useSalt = Boolean.parseBoolean(args[0]);
			if (!useSalt)
			{
				 //tableName += "_no_salt";
				 conf.setBoolean("use_salt", false);
			}
			else
			{
				conf.setBoolean("use_salt", true);
			}
				 
			job = new Job(conf);
			job.setJobName("Bulk loader by file " + tableName);			

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);

			job.setMapperClass(BulkLoaderByFileMapper.class);
			job.setJarByClass(BulkLoaderSkinnyId.class);
			
			job.setInputFormatClass(XmlInputFormat.class);		    
			FileInputFormat.setInputPaths(job, "/xml/input_512");
		
			job.setOutputFormatClass(HFileOutputFormat.class);
			HFileOutputFormat.setOutputPath(job, new Path("/import-data"));
			
			Configuration hConfig = HBaseConfiguration.create(conf);
			//createTable(conf);
			conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName());
	        hConfig.setLong("version", System.currentTimeMillis());	        	        
	        
	        
	        HFileOutputFormat.configureIncrementalLoad(job, new HTable(hConfig, tableName));
		
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


			ToolRunner.run(new BulkLoaderSkinnyId(), args);



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
			  admin.disableTable(Bytes.toBytes(tableName));
			  admin.deleteTable(Bytes.toBytes(tableName));
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  
		  }
		  
		  HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(tableName));
		   
		  //table.addFamily(new HColumnDescriptor(Bytes.toBytes("h"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("h"), 1, "snappy", true, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  //table.addFamily(new HColumnDescriptor(Bytes.toBytes("mr"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("m"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("cp"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("p"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("c"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  
		  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
		  
		  
		  byte[][] splits;
		  int i = 0;
		  
		  if (useSalt){
			  splits = new byte[(NUMBER_OF_NODES)*6][];			  
			  for (byte salt = 0; salt < NUMBER_OF_NODES; salt++)
			  {		  
				 
				  GregorianCalendar cal = new GregorianCalendar();
				  Date date;
				  //splits[i++] = new byte[] {(byte)salt};
				  	
				  date = df.parse("2011-02-13T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.add(Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis())));
				  
				  date = df.parse("2011-05-13T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
				  
				  date = df.parse("2011-05-22T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
				  
				  date = df.parse("2011-05-27T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
				  
				  date = df.parse("2011-06-05T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.toBytes(cal.getTimeInMillis()));
				  
				  date = df.parse("2011-06-13T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));			  
				 
			  }		  
		  }
		  else
		  {
			  
			  splits = new byte[826][];
			  //List<byte[]> splitsList = new ArrayList<byte[]>();
			  GregorianCalendar cal = new GregorianCalendar();
			  
			  String [] heavyLoads = new String[] {
					  "2011-01-23",
					  "2011-01-26",
					  "2011-01-27",
					  "2011-01-28",
					  "2011-01-29",
					  "2011-01-30",
					  "2011-01-31",
					  "2011-02-01",
					  "2011-02-02",
					  "2011-02-03",
					  "2011-02-04",
					  "2011-02-05",
					  "2011-02-06",
					  "2011-02-07",
					  "2011-02-08",
					  "2011-02-09",
					  "2011-02-10",
					  "2011-02-11",
					  "2011-02-12",
					  "2011-03-14",
					  "2011-03-17",
					  "2011-03-23",
					  "2011-03-24",
					  "2011-03-27",
					  "2011-04-09",
					  "2011-04-26",
					  "2011-04-27",
					  "2011-04-30",
					  "2011-05-01",
					  "2011-05-06",
					  "2011-05-07",
					  "2011-05-08",
					  "2011-05-09",
					  "2011-05-10",
					  "2011-05-11",
					  "2011-05-12",
					  "2011-05-19",
					  "2011-05-20",
					  "2011-05-21",
					  "2011-05-22",
					  "2011-05-23",
					  "2011-05-24",
					  "2011-05-25",
					  "2011-05-26",
					  "2011-05-27",
					  "2011-05-28",
					  "2011-05-29",
					  "2011-05-30",
					  "2011-05-31",
					  "2011-06-01",
					  "2011-06-02",
					  "2011-06-03",
					  "2011-06-04",
					  "2011-06-05",
					  "2011-06-06",
					  "2011-06-07",
					  "2011-06-08",
					  "2011-06-09",
					  "2011-06-10"  };
			  
			  for (String day : heavyLoads)
			  {			 
				  for (int hour = 0; hour < 24; hour+=2)
				  {
					  Date date = df.parse(day + "T" + (hour < 10 ? "0" : "") + hour +":00:00 +0000");
					  cal.setTime(date);
					  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
				  }
				  Date date = df.parse(day + "T00:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
				  
				  date = df.parse(day + "T12:00:00 +0000");
				  cal.setTime(date);
				  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
			  }
			  
			 
			  /*
			  date = df.parse("2011-01-29T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-01-31T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-02-02T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-02-06T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-02-08T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  			  
			  date = df.parse("2011-04-30T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-03T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-07T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-09T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-11T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-13T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-19T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-21T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-23T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-25T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  			  
			  date = df.parse("2011-05-27T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-28T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  				  
			  date = df.parse("2011-05-29T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));			  
			  
			  date = df.parse("2011-06-01T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));		
			  
			  date = df.parse("2011-06-03T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-05T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-06T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-08T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-10T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  
			  
			  date = df.parse("2011-01-27T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-05-02T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-03T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-06T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
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
