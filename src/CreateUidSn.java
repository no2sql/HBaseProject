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
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

public class CreateUidSn extends Configured implements Tool {



	static class Mapper1 extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put> {

		private final IntWritable ONE = new IntWritable(1);
		private Text textKey = new Text();
		private Text textVal = new Text();

		static private JAXBContext jc;
		static private Unmarshaller u;
		static private HashMap<String,Set<String>> prevValues= new HashMap<String,Set<String>>();


		@Override
		public void map(LongWritable row, Text value, Context context) throws IOException {


			try {

				String val = value.toString();
				int id = Integer.parseInt(val.substring(0,val.indexOf(',')));
				val = val.substring(val.indexOf(',')+2);
				val = val.substring(0,val.length()-1);


				Put put = new Put(Bytes.toBytes(id));
				put.add(Bytes.toBytes("name"), Bytes.toBytes("s"), Bytes.toBytes(val.toString()));
				put.setWriteToWAL(false);
				context.write(null, put);

				put = new Put(Bytes.toBytes(val.toString()));
				put.add(Bytes.toBytes("id"), Bytes.toBytes("s"), Bytes.toBytes(id));
				put.setWriteToWAL(false);
				context.write(null, put);


			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException (e);
			}

		}

	}



		/**
		 * @param args
		 */
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
				conf.set("mapred.map.tasks.speculative.execution", "false");			
				

				//conf.set("mapred.job.tracker", "local");
				//conf.set("fs.default.name", "local");

				job = new Job(conf);
				job.setJobName("CreateUidSn");			

				job.setJarByClass(CreateUidSn.class);     // class that contains mapper and reducer		

				job.setMapperClass(Mapper1.class);
				job.setMapOutputKeyClass(ImmutableBytesWritable.class);
				job.setMapOutputValueClass(Put.class);


				//job.setOutputKeyClass(IntWritable.class);
				//job.setOutputValueClass(Text.class);
				//job.setOutputKeyClass(ImmutableBytesWritable.class);
				//job.setOutputValueClass(Put.class);

				job.setInputFormatClass(TextInputFormat.class);		    
				FileInputFormat.setInputPaths(job, "/serial_numbers.csv");

				//job.setOutputFormatClass(SequenceFileOutputFormat.class);
				//SequenceFileOutputFormat.setOutputPath(job, new Path("/unique_values"));				
				TableMapReduceUtil.initTableReducerJob(
						"test_uid",      // output table
						null,             // reducer class
						job);
					job.setNumReduceTasks(0);
				


				TableMapReduceUtil.addDependencyJars(job);
				/*
				Configuration hConfig = HBaseConfiguration.create(conf);		
				createTable(conf);
				 */
				/*conf.setInt("mapred.reduce.tasks",4);
			conf.setInt("mapreduce.reduce.tasks",4);
			job.setNumReduceTasks(4);
				 */			

				boolean b = job.waitForCompletion(true);
				if (!b) {
					throw new IOException("error with job!");
				}

				/*
			job = new Job(conf);
			job.setJobName("CreateUid Step2");			

			job.setJarByClass(CreateUid.class);     // class that contains mapper and reducer		

			job.setMapOutputKeyClass(ImmutableBytesWritable.class);
			job.setMapOutputValueClass(Put.class);

		    job.setInputFormatClass(SequenceFileInputFormat.class);		    
		    SequenceFileInputFormat.setInputPaths(job, "/unique_values");

			TableMapReduceUtil.addDependencyJars(job);
				 */



				//job.setOutputKeyClass(ImmutableBytesWritable.class);
				//job.setOutputValueClass(Put.class);

				/*
		    job.setOutputFormatClass(TableOutputFormat.class);
			conf.set(TableOutputFormat.OUTPUT_TABLE, "test_uid");

			job.setOutputFormatClass(HFileOutputFormat.class);
			HFileOutputFormat.setOutputPath(job, new Path("/import-data-uid"));

			Configuration hConfig = HBaseConfiguration.create(conf);		
			createTable(conf);
			conf.set("hfile.compression", Compression.Algorithm.SNAPPY.getName());
	        hConfig.setLong("version", System.currentTimeMillis());	        	        


	        HFileOutputFormat.configureIncrementalLoad(job, new HTable(hConfig, "test_uid"));


			TableMapReduceUtil.addDependencyJars(job);

			conf.setInt("mapred.reduce.tasks",4);
			conf.setInt("mapreduce.reduce.tasks",4);
			job.setNumReduceTasks(4);


			b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
				 */			
				//System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
				return 0;
			}
			catch (Exception e)
			{
				System.out.println("Error: " + e.getMessage());
				e.printStackTrace();
			}

			return -1;

		}

		public static void main(String[] args) {



			long queryStart = System.currentTimeMillis();
			try {
				// Criteria


				ToolRunner.run(new CreateUidSn(), args);



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
			  admin.disableTable(Bytes.toBytes("test_uid"));
			  admin.deleteTable(Bytes.toBytes("test_uid"));
		  }
		  catch (Exception e) {
			  e.printStackTrace();

		  }

		  HTableDescriptor table = new HTableDescriptor(Bytes.toBytes("test_uid"));

		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("id"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("name"), 1, "snappy", true, true, 131072, 2147483647, StoreFile.BloomType.ROWCOL.toString(), HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));		  

		  /*

		  SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");

		  byte[][] splits = new byte[6][];
		  int i = 0;
		  splits[i++] = Bytes.toBytes(3);		 
		  splits[i++] = Bytes.toBytes(6);			  
		  splits[i++] = Bytes.toBytes(9);
		  splits[i++] = Bytes.toBytes("C");
		  splits[i++] = Bytes.toBytes("I");
		  splits[i++] = Bytes.toBytes("N");
		  */
		  //admin.createTable( table, splits );
		  admin.createTable( table);
	    return true;
	  } catch (TableExistsException e) {
		  e.printStackTrace();
	    // the table already exists...
	    return false;  
	  }
	}

	}
