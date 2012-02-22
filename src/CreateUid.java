import net.sigmaquest.supplychain.model.TestRun;
import net.sigmaquest.supplychain.model.UnitReport;
import net.sigmaquest.supplychain.model.ValueBase;
import net.sigmaquest.supplychain.model.ValueBoolean;
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

public class CreateUid extends Configured implements Tool {

	
	
	static class Mapper1 extends Mapper<LongWritable, Text, Text, Text> {

		private final IntWritable ONE = new IntWritable(1);
		private Text textKey = new Text();
		private Text textVal = new Text();

		static private JAXBContext jc;
		static private Unmarshaller u;
		static private HashMap<String,Set<String>> prevValues= new HashMap<String,Set<String>>();

		static {
			try {
				jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
				
				prevValues.put("e", new HashSet<String>());
				prevValues.put("g", new HashSet<String>());
				prevValues.put("l", new HashSet<String>());
				prevValues.put("o", new HashSet<String>());
				prevValues.put("f", new HashSet<String>());
				prevValues.put("s", new HashSet<String>());
				prevValues.put("p", new HashSet<String>());
				prevValues.put("t", new HashSet<String>());
				prevValues.put("mg", new HashSet<String>());
				prevValues.put("mn", new HashSet<String>());
				prevValues.put("mu", new HashSet<String>());
				prevValues.put("mlsl", new HashSet<String>());
				prevValues.put("musl", new HashSet<String>());
				prevValues.put("mv", new HashSet<String>());
				prevValues.put("pg", new HashSet<String>());
				prevValues.put("pn", new HashSet<String>());
				prevValues.put("pu", new HashSet<String>());
				prevValues.put("plsl", new HashSet<String>());
				prevValues.put("pusl", new HashSet<String>());
				prevValues.put("pv", new HashSet<String>());
				
			} catch (Exception e1) {
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
		}
		
		@Override
		public void map(LongWritable row, Text value, Context context) throws IOException {
			

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
				unitReport.setFilename("??????????");


				emitColumnValues(unitReport, unitReport.getTestRun(),null, context);
				
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException (e);
			}

		}


		private void emitColumnValues(UnitReport unitReport, TestRun testRun, TestRun parentTestRun, Context context) throws IOException, InterruptedException
		{
			
			if (testRun.getEndTime() != null && !prevValues.get("e").contains(testRun.getEndTime()))
			{
				textKey.set("e");
				textVal.set(testRun.getEndTime());
				context.write(textKey, textVal);
				prevValues.get("e").add(testRun.getEndTime());			
				context.getCounter("camstar","e").increment(1l);
			}
			
			if (parentTestRun == null && unitReport.getEndTime() != null && !prevValues.get("e").contains(unitReport.getEndTime()))
			{								
				textKey.set("e");
				textVal.set(unitReport.getEndTime());
				context.write(textKey, textVal);
				context.getCounter("camstar","e").increment(1l);
			}

			
			if (!prevValues.get("g").contains(testRun.getGrade())){
				textKey.set("g");
				textVal.set(testRun.getGrade());
				context.write(textKey, textVal);
				prevValues.get("g").add(testRun.getGrade());
				context.getCounter("camstar","g").increment(1l);
			}
			//System.out.println("MAP: g: " + testRun.getGrade());
		
			

			if (parentTestRun == null && !prevValues.get("l").contains(unitReport.getStation().getGuid()))
			{
				textKey.set("l");
				textVal.set(unitReport.getStation().getGuid());
				context.write(textKey, textVal);
				prevValues.get("l").add(unitReport.getStation().getGuid());
				context.getCounter("camstar","l").increment(1l);
			}

			if (parentTestRun == null && !prevValues.get("o").contains(unitReport.getOperator().getName()))
			{
				textKey.set("o");
				textVal.set(unitReport.getOperator().getName());
				context.write(textKey, textVal);
				prevValues.get("o").add(unitReport.getOperator().getName());
				context.getCounter("camstar","o").increment(1l);
			}

			if (parentTestRun == null && !prevValues.get("f").contains(unitReport.getFilename()))
			{
				textKey.set("f");
				textVal.set(unitReport.getFilename());
				context.write(textKey, textVal);
				prevValues.get("f").add(unitReport.getFilename());
				context.getCounter("camstar","f").increment(1l);
			}
/*
			if (parentTestRun == null && !prevValues.get("s").contains(unitReport.getProduct().getSerialNo()))
			{
				textKey.set("s-"+ unitReport.getProduct().getSerialNo().substring(0,2));
				textVal.set(unitReport.getProduct().getSerialNo());
				context.write(textKey, textVal);
				prevValues.get("s").add(unitReport.getProduct().getSerialNo());
				context.getCounter("camstar","SerialNo " + unitReport.getProduct().getSerialNo().charAt(0)).increment(1l);
			}
*/
			if (parentTestRun == null && !prevValues.get("p").contains(unitReport.getProduct().getPartNo()))
			{
				textKey.set("p");
				textVal.set(unitReport.getProduct().getPartNo());
				context.write(textKey, textVal);
				prevValues.get("p").add(unitReport.getProduct().getPartNo());
				context.getCounter("camstar","p").increment(1l);
			}

			if (!prevValues.get("t").contains(testRun.getName()))
			{
				textKey.set("t");
				textVal.set(testRun.getName());
				context.write(textKey, textVal);
				prevValues.get("t").add(testRun.getName());
				context.getCounter("camstar","t").increment(1l);
			}

			if (testRun.getResult() != null)
			{
				for (Object measure : testRun.getResult().getValueDoubleOrValueIntegerOrValueString())
				{
					ValueBase value = (ValueBase) measure;
					if (value.getNameString() != null)
					{
						
						if (value.getGradeString() != null && !prevValues.get("mg").contains(value.getGradeString()))
						{
							textKey.set("mg");
							textVal.set(value.getGradeString());
							context.write(textKey, textVal);
							prevValues.get("mg").add(value.getGradeString());
							context.getCounter("camstar","mg").increment(1l);
						}
						
						if (value.getNameString() != null && !prevValues.get("mn").contains(value.getNameString()))
						{
							textKey.set("mn");
							textVal.set(value.getNameString());
							context.write(textKey, textVal);
							prevValues.get("mn").add(value.getNameString());
							context.getCounter("camstar","mn").increment(1l);
						}

						if (value.getUslString() != null && !prevValues.get("musl").contains(value.getUslString()))
						{
							textKey.set("musl");
							textVal.set(value.getUslString());
							context.write(textKey, textVal);
							prevValues.get("musl").add(value.getUslString());
							context.getCounter("camstar","musl").increment(1l);
						}

						if (value.getLslString() != null && !prevValues.get("mlsl").contains(value.getLslString()))
						{
							textKey.set("mlsl");
							textVal.set(value.getLslString());
							context.write(textKey, textVal);
							prevValues.get("mlsl").add(value.getLslString());
							context.getCounter("camstar","mlsl").increment(1l);
						}

						if (value.getUnitString() != null && !prevValues.get("mu").contains(value.getUnitString()))
						{
							textKey.set("mu");
							textVal.set(value.getUnitString());
							context.write(textKey, textVal);
							prevValues.get("mu").add(value.getUnitString());
							context.getCounter("camstar","mu").increment(1l);
						}

				
						if (value.getValueString() != null && (value instanceof ValueString || value instanceof ValueBoolean) 
								&& !prevValues.get("mv").contains(value.getValueString()))
						{						
							// This is a String, so we need to insert it into the UID table
							textKey.set("mv");
							textVal.set(value.getValueString());
							context.write(textKey, textVal);
							prevValues.get("mv").add(value.getValueString());
							context.getCounter("camstar","mv").increment(1l);
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
					if (value.getNameString() != null)
					{
						
						
						if (value.getNameString() != null && !prevValues.get("pn").contains(value.getNameString()))
						{
							textKey.set("pn");
							textVal.set(value.getNameString());
							context.write(textKey, textVal);
							prevValues.get("pn").add(value.getNameString());
							context.getCounter("camstar","pn").increment(1l);
						}

						if (value.getUslString() != null && !prevValues.get("pusl").contains(value.getUslString()))
						{
							textKey.set("pusl");
							textVal.set(value.getUslString());
							context.write(textKey, textVal);
							prevValues.get("pusl").add(value.getUslString());
							context.getCounter("camstar","pusl").increment(1l);
						}

						if (value.getLslString() != null && !prevValues.get("plsl").contains(value.getLslString()))
						{
							textKey.set("plsl");
							textVal.set(value.getLslString());
							context.write(textKey, textVal);
							prevValues.get("plsl").add(value.getLslString());
							context.getCounter("camstar","plsl").increment(1l);
						}

						if (value.getUnitString() != null && !prevValues.get("pu").contains(value.getUnitString()))
						{
							textKey.set("pu");
							textVal.set(value.getUnitString());
							context.write(textKey, textVal);
							prevValues.get("pu").add(value.getUnitString());
							context.getCounter("camstar","pu").increment(1l);
						}

					
						if (value.getValueString() != null && (value instanceof ValueString || value instanceof ValueBoolean) 
								&& !prevValues.get("pv").contains(value.getValueString()))
						{
							// This is a String, so we need to insert it into the UID table
							textKey.set("pv");
							textVal.set(value.getValueString());
							context.write(textKey, textVal);
							prevValues.get("pv").add(value.getValueString());
							context.getCounter("camstar","pv").increment(1l);
						}
					

					}

				}
			}

			
			if (testRun.getSubtests() != null)
			{
				for (TestRun subtest: testRun.getSubtests())
				{
					emitColumnValues(unitReport, subtest, testRun, context);					
				}
			}

		}

	}

	public static class Combiner extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException 
        {
            //System.out.println("Reducing " + key.toString());
        	
        	Set<String> prevValues = new HashSet<String>();
        	
        	int id = 1;
            for (Text val : values) {
            	
            	
            	//System.out.println("COMBINE: " + key + ": " + val);
            	
            	if (!prevValues.contains(val.toString()))
            	{
            		prevValues.add(val.toString());
	            	context.write(key, val);
	            	//System.out.println("COMBINE: WROTE " + key + ": " + val);
	          	
	            	
            	}
            }
            
        }
    }
	
    public static class Reducer1 extends TableReducer<Text, Text, ImmutableBytesWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException 
        {
            //System.out.println("Reducing " + key.toString());
        	
        	Set<String> prevValues = new HashSet<String>();
        	       	
        	
        	System.out.println("Reducing key " + key);
        	int id = 1;
            for (Text val : values) {
            	
            	//System.out.println("REDUCE: " + key + ": " + val);
            	
            	if (!prevValues.contains(val.toString()))
            	{
	            	Put put = new Put(Bytes.toBytes(id));
	                put.add(Bytes.toBytes("name"), Bytes.toBytes(key.toString()), Bytes.toBytes(val.toString()));
	                put.setWriteToWAL(false);
	                context.write(null, put);
	                
	                put = new Put(Bytes.toBytes(val.toString()));
	                put.add(Bytes.toBytes("id"), Bytes.toBytes(key.toString()), Bytes.toBytes(id));
	                put.setWriteToWAL(false);
	                context.write(null, put);
	                
            		//context.write(new IntWritable(id), new Text(key.toString() + "-" + val.toString()));
	                id++;
	                prevValues.add(val.toString());
	                context.getCounter("camstar","REDUCE " + key + " + VAL counter").increment(1l);
	                if (id % 50000 == 0){	                		               	                	
	                	System.out.println("Reduced " + id);
	                }
	               
            	}
            }
      
            
        }
    }
	
    /*
    static class Mapper2 extends Mapper<IntWritable, Text, ImmutableBytesWritable, Put> {

		private final IntWritable ONE = new IntWritable(1);
		private Text textKey = new Text();
		private Text textVal = new Text();

		static private JAXBContext jc;
		static private Unmarshaller u;

		static {
			try {
				jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
			} catch (Exception e1) {
				System.out.println(e1.getMessage());
				e1.printStackTrace();
			}
		}
		
		@Override
		public void map(IntWritable row, Text value, Context context) throws IOException {
			

			try {
				
				String key = value.toString().substring(0,value.toString().indexOf('-'));
				String val = value.toString().substring(value.toString().indexOf('-')+1,value.toString().length());
			
				Put put = new Put(Bytes.toBytes(row.get()));
                put.add(Bytes.toBytes("name"), Bytes.toBytes(key.toString()), Bytes.toBytes(val.toString()));
                put.setWriteToWAL(false);
                context.write(null, put);
                
                put = new Put(Bytes.toBytes(val.toString()));
                put.add(Bytes.toBytes("id"), Bytes.toBytes(key.toString()), Bytes.toBytes(row.get()));
                put.setWriteToWAL(false);
                context.write(null, put);
			
			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				throw new RuntimeException (e);
			}

		}
    }
    */
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
			conf.set(XmlInputFormat.START_TAG_KEY,"<UnitReport");
			conf.set(XmlInputFormat.END_TAG_KEY,"</UnitReport>");
			
			
			//conf.set("mapred.job.tracker", "local");
			//conf.set("fs.default.name", "local");
						
			job = new Job(conf);
			job.setJobName("CreateUid");			

			job.setJarByClass(CreateUid.class);     // class that contains mapper and reducer		
			
			job.setMapperClass(Mapper1.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setCombinerClass(Combiner.class);
			
			job.setReducerClass(Reducer1.class);
			//job.setOutputKeyClass(IntWritable.class);
		    //job.setOutputValueClass(Text.class);
			//job.setOutputKeyClass(ImmutableBytesWritable.class);
		    //job.setOutputValueClass(Put.class);
		    
		    job.setInputFormatClass(XmlInputFormat.class);		    
			FileInputFormat.setInputPaths(job, "/xml/input_512");
		    
			//job.setOutputFormatClass(SequenceFileOutputFormat.class);
			//SequenceFileOutputFormat.setOutputPath(job, new Path("/unique_values"));
			
			TableMapReduceUtil.initTableReducerJob(
					"test_uid",      // output table
					Reducer1.class,             // reducer class
					job);
			
			TableMapReduceUtil.addDependencyJars(job);
			
			/*Configuration hConfig = HBaseConfiguration.create(conf);		
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


			ToolRunner.run(new CreateUid(), args);



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
