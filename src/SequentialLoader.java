import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import net.sigmaquest.supplychain.model.Category;
import net.sigmaquest.supplychain.model.Operator;
import net.sigmaquest.supplychain.model.Product;
import net.sigmaquest.supplychain.model.Property;
import net.sigmaquest.supplychain.model.Result;
import net.sigmaquest.supplychain.model.SetUp;
import net.sigmaquest.supplychain.model.Station;
import net.sigmaquest.supplychain.model.TestRun;
import net.sigmaquest.supplychain.model.UnitReport;
import net.sigmaquest.supplychain.model.ValueArray;
import net.sigmaquest.supplychain.model.ValueAttachment;
import net.sigmaquest.supplychain.model.ValueBase;
import net.sigmaquest.supplychain.model.ValueBoolean;
import net.sigmaquest.supplychain.model.ValueDouble;
import net.sigmaquest.supplychain.model.ValueInteger;
import net.sigmaquest.supplychain.model.ValueRecord;
import net.sigmaquest.supplychain.model.ValueString;
import net.sigmaquest.supplychain.model.ValueTimestamp;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Blob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.StringTokenizer;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SequentialLoader {

	static private JAXBContext jc;
	static private Unmarshaller u;
	public static final int NUMBER_OF_REGION_SERVERS = 4;
	public static int NUMBER_OF_CLIENTS = 1;

	private static Long testRunId = 0l;
	
	private static boolean doneReading = false;
	
	private static Long num = 0l;
	private static Long secs = 0l;

	private static Long numPuts = 0l;
	
	public static HTable createHTable(String filepath) throws IOException, JAXBException {
		Configuration conf = HBaseConfiguration.create();
		
		HTable table = new HTable(conf, "test");
		table.setAutoFlush(false);
		table.setWriteBufferSize(1024*1024*24);

		File dir = new File(filepath);

		File[] files = dir.listFiles();
				
		int testNbr = 1;
		
		for (File file : files)		
		{
			
			if (file.isDirectory())
				continue;
			
			String xml = FileUtils.readFileToString(file);			
			UnitReport unitReport = (UnitReport) u.unmarshal(new StringReader(xml));
			unitReport.setFilename(file.getName());
			
			try {				
				//putTest(table, unitReport, unitReport.getTestRun(), null);							
				System.out.println("Loaded test " + testNbr++);			
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		table.flushCommits();
		return table;
	}
	
	
	//private static Queue<UnitReport> createQueue(ResultSet rs){
		
	public static HTable createHTable(ResultSet rs)
	{
			
		
		try {	
			Configuration conf = HBaseConfiguration.create();
			HTable table = new HTable(conf, "test");
			table.setAutoFlush(false);
			table.setWriteBufferSize(1024*1024*24);
			
			char[] uncompress = new char[65000];
				InputStreamReader isr = null;
				int count = 0;
				int errcount = 0;
				long starttime = System.currentTimeMillis();
				long stime = System.currentTimeMillis();
				long etime = 0;
				while (rs.next())		
				{
					
					Blob filedata = rs.getBlob(2);
					InputStream in = filedata.getBinaryStream();
					ZipInputStream zis = new ZipInputStream(in);
					ZipEntry entry = zis.getNextEntry();
					isr = new InputStreamReader(zis);

					for (int i =0; i< uncompress.length; i++){
						uncompress[i] =' ';			
					}

					isr.read(uncompress);
					String xml = new String(uncompress);
					xml = xml.trim();
				

					String[] xmls = StringUtils.splitByWholeSeparator(xml, "<UnitReport ");
					
					for (int i = 0; i < xmls.length; i++)						
					{
						
						if (i == 0)
							continue;
						
						String singleXml = "<UnitReport " + xmls[i];
						
						if (singleXml.indexOf("urn:unitreport-schema") == -1)
						{
	
							String sub = new String("xmlns");
							int index = singleXml.indexOf(sub);
							String f1 = singleXml.substring(0,index);
							String f2 = singleXml.substring(index);
							//String newxml = f1.concat(" xmlns=\"urn:unitreport-schema\" ").concat(f2);
							singleXml = f1 + " xmlns=\"urn:unitreport-schema\" " + f2;
						}
	
	
						count++;
									
						if (singleXml.indexOf("</UnitReportList") != -1)
						{												
							singleXml = singleXml.substring(0, singleXml.indexOf("</UnitReportList"));
						}
						
						UnitReport unitReport = (UnitReport) u.unmarshal(new StringReader(singleXml));
						unitReport.setFilename(rs.getString(1));
						
						try {				
							putTest(table, unitReport, unitReport.getTestRun(), null);
							
							//System.out.println("Loaded test " + count++);			
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						
					}
					if(count%10000==0){
						etime = System.currentTimeMillis();
						long time = (etime-stime)/1000;
						System.out.println("Time load = "+ time);
						System.out.println("Number of records ="+count);
						float rate = (float)count/(float)time;
						stime = System.currentTimeMillis();
						System.out.println("The loading rate is "+ rate);
					}
				}
		        long endtime = System.currentTimeMillis();
				long totaltime = (endtime-starttime)/1000;
				System.out.println("Time load = "+ totaltime);
				System.out.println("Number of records ="+count);
				float rate = (float)count/(float)totaltime;
				System.out.println("The loading rate is "+ rate);
				
				table.flushCommits();
				return table;
			//}
			} 
			catch (Exception e)
			{
				System.out.println("Error " + e.getMessage());
				e.printStackTrace();
			}	
		return null;
		
		}
	

	
								
		private static Long putTest(HTable table, UnitReport unitReport, TestRun testRun, TestRun parentTestRun) throws IOException
		{				
			try {
				synchronized (testRunId)
				{
					testRun.setTestId(++testRunId);
				}
				//System.out.println(testRunId);
			
			byte prefix = (byte) (unitReport.getStartTime().hashCode() % NUMBER_OF_REGION_SERVERS);
			System.out.println("Prefix: " + prefix + " for " + unitReport.getStartTime());
			byte[] rowkey = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(
					"-".concat(testRun.getStartTime()).concat("-").
					concat(unitReport.getMode()).concat("-").
					concat(parentTestRun == null ? "1" : "0").concat("-").
					concat(testRun.getGrade()).concat("-").
					concat(unitReport.getStation().getName()).concat("-").				
					concat(String.valueOf(testRun.getTestId()))
				));
			
			Put put = new Put(rowkey);
			// put column family -header
			if (testRun.getEndTime() != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes("Endtime"),
					Bytes.toBytes(testRun.getEndTime()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("Filename"),
					Bytes.toBytes(unitReport.getFilename()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("Serialnumber"),
					Bytes.toBytes(unitReport.getProduct().getSerialNo()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("ProductID"),
					Bytes.toBytes(unitReport.getProduct().getPartNo()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("OperatorID"),
					Bytes.toBytes(unitReport.getOperator().getName()));
			put.add(Bytes.toBytes("h"), Bytes.toBytes("TestID"),
					Bytes.toBytes(testRun.getName()));
			if (unitReport.getVersion() != null)
				put.add(Bytes.toBytes("h"), Bytes.toBytes("VersionID"),
					Bytes.toBytes(unitReport.getVersion()));
			if (unitReport.getQuantity() != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes("Quantity"),
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
						put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-LSL"),
								Bytes.toBytes(value.getLslString()));
						if (value.getUslString() != null)
						put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-USL"),
								Bytes.toBytes(value.getUslString()));
						if (value.getNameString() != null)
						put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-MeasureName"),
								Bytes.toBytes(value.getNameString()));
						if (value.getUnitString() != null)
						put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-MeasureUnit"),
								Bytes.toBytes(value.getUnitString()));
						if (value.getGradeString() != null)
							put.add(Bytes.toBytes("m"), Bytes.toBytes(value.getNameString() + "-Grade"),
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
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-LSL"),
							Bytes.toBytes(value.getLslString()));
					if (value.getUslString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-USL"),
							Bytes.toBytes(value.getUslString()));
					if (value.getNameString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-MeasureName"),
							Bytes.toBytes(value.getNameString()));
					if (value.getUnitString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-MeasureUnit"),
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
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-LSL"),
							Bytes.toBytes(value.getLslString()));
					if (value.getUslString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-USL"),
							Bytes.toBytes(value.getUslString()));
					if (value.getNameString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-MeasureName"),
							Bytes.toBytes(value.getNameString()));
					if (value.getUnitString() != null)
					put.add(Bytes.toBytes("cp"), Bytes.toBytes(value.getNameString() + "-MeasureUnit"),
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
					Long subtestId = putTest(table, unitReport, subtest, testRun);
					put.add(Bytes.toBytes("c"), Bytes.toBytes(subtestId), Bytes.toBytes(subtest.getGrade()));
				}
			}
			
			put.setWriteToWAL(false);
			table.put(put);		
			numPuts++;
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
		
	
	
	
	
	
	
	public static void main(String [] args)
	{
		try {
			jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}

		try {
			u = jc.createUnmarshaller();			
		} catch (Exception e2) {
			System.out.println(e2.getMessage());
			e2.printStackTrace();
		}
		
		
		 String start = null;
        String end = null;
		if(args[0] != null){
			start = args[0];		
		}else{
			System.out.println("missing start time string!");
			start = "13-FEB-2011";
		}
		if(args[1] != null){		
			end = args[1];
		}else{
			System.out.println("missing end time string!");	
			end = "14-FEB-2011";
		}
		
		if(args[2] != null){		
			NUMBER_OF_CLIENTS = Integer.parseInt(args[2]);
			
		}else{
			System.out.println("Missing number of clients");				
		}
		
		System.out.println("Using " + NUMBER_OF_CLIENTS + " clients");
				
		Connection con = null;
		String query = "select filename,filedata from SCHATURVEDI_711.sq_unit_report_compressed_file where id "
				+ "IN (select id from SCHATURVEDI_711.sq_unit_report  where starttime >= "
				+ "to_date('"+start+"','DD-MON-YYYY') AND starttime < to_date('"+end+"','DD-MON-YYYY'))";
		try {
			con = JdbcConnection.getOracleJDBCConnection();
			Statement statement = con.createStatement();
			statement.setFetchSize(1000);
			ResultSet rs = statement.executeQuery(query);		
			HTable table = createHTable(rs);
			//BlockingQueue<UnitReport> queue = new ArrayBlockingQueue <UnitReport>(NUMBER_OF_CLIENTS);	
			
			//UnitReportReader reader =  (new CopyOfLoader2()).new UnitReportReader(queue, rs);
			//Thread readerThread = new Thread(reader);
			//readerThread.start();
			
			//System.out.println("Queue size is " + queue.size());
			
			rs.close();
			statement.close();
			
			//for (int i = 0; i < NUMBER_OF_CLIENTS; i++)
			//{
			//	UnitReportWriter writer =  (new CopyOfLoader2()).new UnitReportWriter(queue);
			//	Thread writerThread = new Thread(writer);
			//	writerThread.start();
			//}
			
			
			
			//HTable table = createHTable("C:\\Users\\pcliu.CAMSTAR\\Desktop\\xml");
		} catch (Exception e3) {
			e3.printStackTrace();
		}
		
		
		/*
		try {
			HTable table = createHTable(args[0]);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		//return table;
	}

}
