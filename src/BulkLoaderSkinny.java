import net.sigmaquest.supplychain.model.TestRun;
import net.sigmaquest.supplychain.model.UnitReport;
import net.sigmaquest.supplychain.model.ValueBase;

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
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Random;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Unmarshaller;

public class BulkLoaderSkinny extends Configured implements Tool {

	public static final int NUMBER_OF_NODES = 7;

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

			/*            
        	  ur.productid,
        	  tr.testid,
        	  val.propertyid,
        	  tr.outcomeid
			 */
			
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


			putTest(puts, unitReport, unitReport.getTestRun(),null);

			//System.out.println("Loaded test " + count++);
			
			
			return puts;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException (e);
		}

	}



	private static Long testRunId = 0l;

	public static Long putTest(List<Put> putList, UnitReport unitReport, TestRun testRun, TestRun parentTestRun) throws IOException
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
											new byte[] {(byte)(parentTestRun == null ? 1 : 0)},
											Bytes.add(
													Bytes.toBytes(cal.getTimeInMillis()),
													Bytes.toBytes(unitReport.getTestRun().getTestId()),
													Bytes.toBytes(randomGenerator.nextInt())
										)
									)
							)
					
					);

			
			
			Put put = new Put(rowkey);
					
			// put column family -header
			if (testRun.getEndTime() != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes("end"),
						Bytes.toBytes(testRun.getEndTime()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("gr"),
					Bytes.toBytes(testRun.getGrade()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("st"),
					Bytes.toBytes(unitReport.getStation().getGuid()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("f"),
					Bytes.toBytes(unitReport.getFilename()));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes("sn"),
					Bytes.toBytes(unitReport.getProduct().getSerialNo()));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes("pid"),
					Bytes.toBytes(unitReport.getProduct().getPartNo()));
			
			put.add(Bytes.toBytes("h"), Bytes.toBytes("oid"),
					Bytes.toBytes(unitReport.getOperator().getName()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("tid"),
					Bytes.toBytes(testRun.getName()));
			if (unitReport.getVersion() != null)
				put.add(Bytes.toBytes("h"), Bytes.toBytes("vid"),
						Bytes.toBytes(unitReport.getVersion()));
			if (unitReport.getQuantity() != null)
				put.add(Bytes.toBytes("h"), Bytes.toBytes("qid"),
						Bytes.toBytes(unitReport.getQuantity()));

			//put.add(Bytes.toBytes("h"), Bytes.toBytes("Seq"), Bytes.toBytes(""));
			//put.add(Bytes.toBytes("h"), Bytes.toBytes("Code"), Bytes.toBytes(""));

			// put column family --measurement
			if (testRun.getResult() != null)
			{
				for (Object measure : testRun.getResult().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) measure;
					if (value.getNameString() != null)
					{
						if (value.getLslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-lsl"),
									Bytes.toBytes(value.getLslString()));
						if (value.getUslString() != null)
							put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-usl"),
									Bytes.toBytes(value.getUslString()));										
						if (value.getUnitString() != null)
							put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-mu"),
									Bytes.toBytes(value.getUnitString()));
						if (value.getGradeString() != null)
							put.add(Bytes.toBytes("h"), Bytes.toBytes(value.getNameString() + "-gr"),
									Bytes.toBytes(value.getGradeString()));
						if (value.getValueString() != null)
							put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString()),
									Bytes.toBytes(value.getValueString()));
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
					if (value.getLslString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-lsl"),
								Bytes.toBytes(value.getLslString()));
					if (value.getUslString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-usl"),
								Bytes.toBytes(value.getUslString()));
					if (value.getUnitString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-mu"),
								Bytes.toBytes(value.getUnitString()));
					if (value.getValueString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString()),
								Bytes.toBytes(value.getValueString()));
				}
			}

			if (testRun.getProperty() != null)
			{
				for (Object property : testRun.getProperty().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) property;
					if (value.getLslString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-lsl"),
								Bytes.toBytes(value.getLslString()));
					if (value.getUslString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-usl"),
								Bytes.toBytes(value.getUslString()));
					if (value.getUnitString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-mu"),
								Bytes.toBytes(value.getUnitString()));
					if (value.getValueString() != null)
						put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString()),
								Bytes.toBytes(value.getValueString()));
				}
			}

			// put column family --- parent
			if (parentTestRun != null) {
				put.add(Bytes.toBytes("p"), Bytes.toBytes(parentTestRun.getTestId()),
						Bytes.toBytes(parentTestRun.getGrade()));
			}

			if (testRun.getSubtests() != null)
			{
				for (TestRun subtest: testRun.getSubtests())
				{
					Long subtestId = putTest(putList, unitReport, subtest, testRun);
					put.add(Bytes.toBytes("c"), Bytes.toBytes(subtestId), Bytes.toBytes(subtest.getGrade()));
				}
			}

			put.setWriteToWAL(false);
						
			putList.add(put);		

			//puts.add(put);
			return testRun.getTestId();
		}
		catch (Exception e)
		{
			System.out.println("ERROR: " + e.getMessage());
			e.printStackTrace();
		}
		return null;
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
			job.setJobName("Bulk loader by file test_skinny");			

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);

			job.setMapperClass(BulkLoaderByFileMapper.class);
			job.setJarByClass(BulkLoaderSkinny.class);
			
			job.setInputFormatClass(XmlInputFormat.class);		    
			FileInputFormat.setInputPaths(job, "/xml/input_512");
		
			job.setOutputFormatClass(HFileOutputFormat.class);
			HFileOutputFormat.setOutputPath(job, new Path("/import-data-skinny"));
			
			Configuration hConfig = HBaseConfiguration.create(conf);
			createTable(conf);
			conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName());
	        hConfig.setLong("version", System.currentTimeMillis());	        	        
	        
	        
	        HFileOutputFormat.configureIncrementalLoad(job, new HTable(hConfig, "test_skinny"));
		
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


			ToolRunner.run(new BulkLoaderSkinny(), args);



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
			  admin.disableTable(Bytes.toBytes("test_skinny"));
			  admin.deleteTable(Bytes.toBytes("test_skinny"));
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  
		  }
		  
		  HTableDescriptor table = new HTableDescriptor(Bytes.toBytes("test_skinny"));
		   
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("h"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("m"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("cp"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("p"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("c"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));	  
		  
		  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
		  
		  byte[][] splits = new byte[NUMBER_OF_NODES-2][];
		  int i = 0;
		  for (byte salt = 1; salt < NUMBER_OF_NODES-1; salt++)
		  {		  
			  GregorianCalendar cal = new GregorianCalendar();
			  
			  Date date;
			 
			  /*
			  date = df.parse("2011-02-13T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis()))));
			  	*/		  
			  //date = df.parse("2011-05-13T00:00:00 +0000");
			  //cal.setTime(date);
			  //splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis()))));
			  splits[i++] = new byte[] {(byte)salt};
			  /*
			  date = df.parse("2011-05-23T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			 
			  date = df.parse("2011-05-27T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			  
			  date = df.parse("2011-06-04T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
			 */
			  //date = df.parse("2011-06-13T00:00:00 +0000");
			  //cal.setTime(date);
			  //splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)0}, Bytes.toBytes(cal.getTimeInMillis())));
				/*	  
			  date = df.parse("2011-09-01T00:00:00 +0000");
			  cal.setTime(date);
			  splits[i++] = Bytes.add(new byte[] {(byte)salt}, Bytes.toBytes('P'),Bytes.add(new byte[] {(byte)1}, Bytes.toBytes(cal.getTimeInMillis())));
			  
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
