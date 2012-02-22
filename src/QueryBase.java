import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class QueryBase {

	public static final int NUMBER_OF_NODES = 7;
	
	protected abstract void setScanFilter(Scan scan);
	protected abstract Class<? extends TableMapper> getMapperClass();
	protected abstract Class<? extends Reducer> getCombinerClass();
	protected abstract Class<? extends TableReducer> getReducerClass();
	
	protected abstract Class<? extends TableMapper> getAggregationMapperClass();	
	protected abstract Class<? extends TableReducer> getAggregationReducerClass();
	
	protected abstract Class getMapOutputKeyClass();
	protected abstract Class getMapOutputValueClass();
	protected abstract String getInputTableName();
	protected abstract String getResultTableName();
	
	
	private Configuration conf = HBaseConfiguration.create();
	private Date startDate = null;
	private Date stopDate = null;
	
	public QueryBase(String startDate, String stopDate) throws ParseException
	{
		GregorianCalendar cal = new GregorianCalendar();
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
		if (StringUtils.defaultString(startDate).length() > 0)
		{
			Date date = df.parse(startDate);
			cal.setTime(date);
			cal.set(Calendar.HOUR, 0);
			cal.set(Calendar.MINUTE, 0);
			cal.set(Calendar.SECOND, 0);
			this.startDate = cal.getTime();
			
			date = df.parse(stopDate);
			cal.setTime(date);
			cal.set(Calendar.HOUR, 23);
			cal.set(Calendar.MINUTE, 59);
			cal.set(Calendar.SECOND, 59);
			
			this.stopDate = cal.getTime();
		}
	}
	
	public void truncateResultTable() throws IOException
	{		
		HBaseAdmin admin = new HBaseAdmin(conf);
		  try{
			  admin.disableTable(Bytes.toBytes(getResultTableName()));
			  admin.deleteTable(Bytes.toBytes(getResultTableName()));
			  
			  if (getStartDate() != null)
			  {
				  admin.disableTable(Bytes.toBytes(getResultTableName()+"temp"));
				  admin.deleteTable(Bytes.toBytes(getResultTableName()+"temp"));
			  }
		  }
		  catch (Exception e) {
			  e.printStackTrace();
			  
		  }
		  
		  HTableDescriptor table = new HTableDescriptor(Bytes.toBytes(getResultTableName()));		   		
		  table.addFamily(new HColumnDescriptor(Bytes.toBytes("details"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
		  admin.createTable(table);		 
		  
		  if (getStartDate() != null)
		  {
			  table = new HTableDescriptor(Bytes.toBytes(getResultTableName()+"temp"));		   		
			  table.addFamily(new HColumnDescriptor(Bytes.toBytes("details"), 1, "snappy", false, true, 131072, 2147483647, "NONE", HColumnDescriptor.DEFAULT_REPLICATION_SCOPE));
			  admin.createTable(table);
		  }
	}
	
	private int numComplete = 0;
		
	public void execute() throws IOException
	{
		truncateResultTable();
		
		long queryStart = System.currentTimeMillis();
		numComplete = 0;
		
		try { 
			
			List<Job> jobList = new ArrayList<Job>();
			
			if (getStartDate() == null)
				jobList.add(createJob((byte)0));
			else
			{
				for (byte salt = 0; salt < NUMBER_OF_NODES; salt++)
				{ 
					jobList.add(createJob(salt));
				}
			}
			
			while (numComplete < jobList.size())
			{
				Thread.sleep(1000);
			}			
			
			if (numComplete > 1)
			{
				// Do the aggregation across salts
				doAggregation();
			}
			
			System.out.println("Query ran in " + ((System.currentTimeMillis() - queryStart)/1000) + " seconds");
		}
		catch (Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
		}
	}
	
	private Job createJob(byte salt) throws IOException
	{
		String mode = "D";
		conf.set("mapred.map.tasks.speculative.execution", "false");
		conf.set("mapred.tasktracker.map.tasks.maximum", "4");
		//conf.set("mapred.task.profile", "true");
		//System.out.println("AM profiling");
		conf.set("hadoop.job.history.user.location", "/mapred");
		//conf.set("mapred.task.profile.maps", "4");
		//conf.set("mapred.task.profile.reduces", "4");
		
		Job job = new Job(conf, this.getClass().getName() + salt);
		
		job.setJarByClass(this.getClass());     // class that contains mapper and reducer					
		Scan scan = new Scan();
		scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		/*
		if (getStartDate() != null)
		{
			conf.set("aggregate_by_salt","true");
			SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
			GregorianCalendar cal = new GregorianCalendar();					 			
			cal.setTime(getStartDate());
			scan.setStartRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add(new byte[] {1}, Bytes.toBytes(cal.getTimeInMillis())))));
						
			cal.setTime(getStopDate());
			scan.setStopRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add(new byte[] {1}, Bytes.toBytes(cal.getTimeInMillis())))));
		}*/
		
		GregorianCalendar cal = new GregorianCalendar();		
		if (getStartDate() != null)
		{
			
			if(isTableSkinny){
				System.out.println("Using SKINNY table...");
				cal.setTime(getStartDate());
				scan.setStartRow(Bytes.add((new byte[] {salt}), Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add((new byte[]{(byte) 0}), Bytes.toBytes(cal.getTimeInMillis())))));
			    cal.setTime(getStopDate());
			    scan.setStopRow(Bytes.add((new byte[] {salt}), Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.add((new byte[]{(byte) 0}), Bytes.toBytes(cal.getTimeInMillis())))));
			}
			else{
				System.out.println("Using FAT table...");
				cal.setTime(getStartDate());
				scan.setStartRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.toBytes(cal.getTimeInMillis()))));
			    cal.setTime(getStopDate());
			    scan.setStopRow(Bytes.add(new byte[] {salt}, Bytes.add(Bytes.toBytes(mode.charAt(0)), Bytes.toBytes(cal.getTimeInMillis()))));
			}
			
		}
		//System.out.println("Setting scan columns...");
		//scan.addColumn(Bytes.toBytes("h"), Bytes.toBytes("ProductID"));							
	    //conf.set("mapred.job.tracker", "local");
	    //conf.set("fs.default.name", "local");
	
		setScanFilter(scan);
		
		TableMapReduceUtil.initTableMapperJob(
				getInputTableName(),      // input table
				scan,	          // Scan instance to control CF and attribute selection
				getMapperClass(),   // mapper class
				getMapOutputKeyClass(),	          // mapper output key
				getMapOutputValueClass(),	          // mapper output value
				job,
				true);
		
		if (getCombinerClass() != null)
			job.setCombinerClass(getCombinerClass());
		
		if (getReducerClass() != null)
		TableMapReduceUtil.initTableReducerJob(getResultTableName() + (getStartDate() == null ? "" : "temp"),
				getReducerClass(), job,
				null, null, null,
				null, false);
		
		//TableMapReduceUtil.addDependencyJars(job);
		
		
		new Thread(new JobThread(job)).start();	
		return job;
	}
	
	private boolean isTableSkinny = true;
	
	public void setSkinnyTable(){		
		this.isTableSkinny = true;
	}
	public void setFatTable(){
		this.isTableSkinny = false;	
	}
	
	private void doAggregation() throws IOException, InterruptedException, ClassNotFoundException
	{
		Job job = new Job(conf, this.getClass().getName() + " Aggregation");
		job.setJarByClass(this.getClass());     // class that contains mapper and reducer					
		Scan scan = new Scan();
		scan.setCaching(1000);        // 1 is the default in Scan, which will be bad for MapReduce jobs
		scan.setCacheBlocks(false);  // don't set to true for MR jobs
		
		TableMapReduceUtil.initTableMapperJob(
				getResultTableName()+"temp",      // input table
				scan,	          // Scan instance to control CF and attribute selection
				getAggregationMapperClass(),   // mapper class
				getMapOutputKeyClass(),	          // mapper output key
				getMapOutputValueClass(),	          // mapper output value
				job,
				true);		
		
		if (getReducerClass() != null)
		TableMapReduceUtil.initTableReducerJob(
			getResultTableName(),      // output table
			getAggregationReducerClass(),             // reducer class
			job);
		
		TableMapReduceUtil.addDependencyJars(job);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
	}
	
	public class JobThread implements Runnable
	{
		private Job job = null;
		public JobThread(Job job)
		{
			this.job = job;
		}
		@Override
		public void run() {
			try {
				boolean b = job.waitForCompletion(true);
				if (!b) {
					throw new IOException("error with job!");
				}
			}
			catch (Exception e)
			{
				System.out.println("Error: " + e.getMessage());
				e.printStackTrace();
			}
			numComplete++;			
		}	
	}

	public Date getStartDate() {
		return startDate;
	}
	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}
	public Date getStopDate() {
		return stopDate;
	}
	public void setStopDate(Date stopDate) {
		this.stopDate = stopDate;
	}
	
	
	
	
}
