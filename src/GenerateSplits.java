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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

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

public class GenerateSplits extends Configured implements Tool {

	public static final int NUMBER_OF_NODES = 7;
	
	static {
		try {
			jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}
	}
	
	public static final IntWritable ONE = new IntWritable(1);
	
	static class SplitsMapper extends Mapper<LongWritable, Text, Text, IntWritable>  {
		
		@Override
		public void map(LongWritable row, Text value, Context context) throws IOException {
			
			useSalt = context.getConfiguration().getBoolean("use_salt",true);
		

			try {
				Unmarshaller u = jc.createUnmarshaller();
				
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
				
				processTest(unitReport, unitReport.getTestRun(),null, context);
				
				

				//System.out.println("Loaded test " + count++);
				
				
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException (e);
			}
						
			
		

		}


	}

	static private JAXBContext jc;
	static private Unmarshaller u;

	private static Text key = new Text();
	

	public static void processTest(UnitReport unitReport, TestRun testRun, TestRun parentTestRun, Context context)throws IOException
	{				
		try {
			
			
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
			String startTime = testRun.getStartTime();
			if (startTime.indexOf(".") > -1)
				startTime = startTime.substring(0,startTime.indexOf(".")) + startTime.substring(startTime.indexOf(".")+4, startTime.length());
			startTime = startTime.substring(0,19) + " " + startTime.substring(19,22) + startTime.substring(23,25);
			Date date = df.parse(startTime);
			
			SimpleDateFormat mydf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
			
			key.set((parentTestRun == null?"1":"0")+ "|" + mydf.format(date));
			
			context.write(key, ONE);
			
			
			if (testRun.getSubtests() != null)
			{
				for (TestRun subtest: testRun.getSubtests())
				{
					processTest(unitReport, subtest, testRun, context);						
				}
			}
			
		}
		catch (Exception e)
		{
			System.out.println("ERROR: " + e.getMessage());
			e.printStackTrace();
			throw new RuntimeException (e);
		}
	}



	private static boolean useSalt = true;
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
			job.setJobName("Generate Splits " + tableName);			

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(SplitsMapper.class);
			job.setJarByClass(GenerateSplits.class);
			
			job.setInputFormatClass(XmlInputFormat.class);		    
			FileInputFormat.setInputPaths(job, "/xml/input_512");
		
			job.setOutputFormatClass(TextOutputFormat.class);
			TextOutputFormat.setOutputPath(job, new Path("/datelist"));
			
	        
	        
		
			
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


			ToolRunner.run(new GenerateSplits(), args);



			// set Mapper, etc., and call JobClient.runJob(conf);




			System.out.println("Load ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}

	}




}
