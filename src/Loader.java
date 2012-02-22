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
import net.sigmaquest.supplychain.model.ValueDouble;
import net.sigmaquest.supplychain.model.ValueInteger;
import net.sigmaquest.supplychain.model.ValueString;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.w3c.dom.Document;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.sql.*;

public class Loader {

	static private JAXBContext jc;
	static private Unmarshaller u;
	public static final int number_of_region_servers = 4;

	private static int testid = 0;
	
	public Loader(JAXBContext jac, Unmarshaller us) {
		jc = jac;
		u = us;
	}

	private static ArrayList<Put> createHPutItems(String xml) throws IOException, JAXBException {
        
		ArrayList<Put> putItems = new ArrayList<Put>();
		UnitReport unitreport = new UnitReport();
		unitreport = (UnitReport) u.unmarshal(new StringReader(xml));

		testid++;
		int parentid = testid;
		ArrayList<String> childValueList = new ArrayList<String>();
		ArrayList<String> childColumnList = new ArrayList<String>();
		
		// get all of values out from unitreport object
		String starttime = unitreport.getStartTime();
		String mode = unitreport.getMode();
		String endtime = unitreport.getEndTime();
		String filename = xml;
		String sn = new String();
		String partnumber = new String();
		Category category = unitreport.getCategory();
		List<Object> products = category.getPropertyOrProductOrCategory();
		for (Object o : products) {
			Product pro = (Product) o;
			sn = pro.getSerialNo();
			partnumber = pro.getPartNo();
			break;
		}
		String version = unitreport.getVersion();
		Long quantity = unitreport.getQuantity();
		Station station = unitreport.getStation();
		String stationname = station.getName();
		Operator operator = unitreport.getOperator();
		String operatorname = operator.getName();
		TestRun testrun = unitreport.getTestRun();
		String testname = testrun.getName();
		String measurename = new String();
		String measureunit = new String();
		Double measurelsl = new Double(0);
		Double measureusl = new Double(0);
		double measurevalue = 0;
		String teststarttime = testrun.getStartTime();
		String testendtime = testrun.getEndTime();
		String guid = station.getGuid();
		String outcome = new String();
		
		
		List<Object> objList = unitreport.getTestRun()
				.getPropertyOrSetUpOrResult();
		for (Object o : objList) {
			if (o instanceof SetUp) {
				SetUp setup = (SetUp) o;
				List<Object> objSet = setup
						.getValueDoubleOrValueIntegerOrValueString();
				for (Object obj : objSet) {
					if (obj instanceof ValueDouble) {
						ValueDouble vb = (ValueDouble) obj;	
					}
					if (obj instanceof ValueInteger) {
						ValueInteger vi = (ValueInteger) obj;
					}
					if (obj instanceof ValueString) {
						ValueString vs = (ValueString) obj;						
					}
				}

			}
			if (o instanceof Result) {
				Result rs = (Result) o;
				List<Object> rsSet = rs.getValueDoubleOrValueIntegerOrValueString();
				for (Object obj : rsSet) {
					if (obj instanceof ValueDouble) {
						ValueDouble vb = (ValueDouble) obj;
						measurename = vb.getName();
						measureunit = vb.getUom();
						measurelsl = vb.getLsl();
						measureusl = vb.getUsl();
						measurevalue = vb.getValue();
						outcome = vb.getGrade();
					}
					if (obj instanceof ValueInteger) {
						ValueInteger vi = (ValueInteger) obj;						
					}
					if (obj instanceof ValueString) {
						ValueString vs = (ValueString) obj;						
					}
				}

			}
			if (o instanceof Property) {
				Property py = (Property) o;
				List<Object> pySet = py.getValueDoubleOrValueIntegerOrValueString();
				for (Object obj : pySet) {
					if (obj instanceof ValueDouble) {
						ValueDouble vb = (ValueDouble) obj;						
					}
					if (obj instanceof ValueInteger) {
						ValueInteger vi = (ValueInteger) obj;						
					}
					if (obj instanceof ValueString) {
						ValueString vs = (ValueString) obj;						
					}
				}

			}
			if (o instanceof TestRun) {
				TestRun subtest = (TestRun)o;
				testid++;		        
				Integer testidInt = new Integer(testid);
				
		        String[] childvalues = new String[0];
				String[] childcolumns = new String[0];
				String parentcolumn = (new Integer(parentid)).toString();
				String parentvalue = outcome;
				
		        List<Object> objList1 = subtest.getPropertyOrSetUpOrResult();
		        for(Object o1 : objList1)
		        if (o1 instanceof Result) {
					Result rs1 = (Result) o1;
					List<Object> objSet1 = rs1
							.getValueDoubleOrValueIntegerOrValueString();
					for (Object obj1 : objSet1) {
						if (obj1 instanceof ValueDouble) {
							ValueDouble vb1 = (ValueDouble) obj1;
							measurename = vb1.getName();
							measureunit = vb1.getUom();
							measurelsl = vb1.getLsl();
							measureusl = vb1.getUsl();
							measurevalue = vb1.getValue();
							outcome = vb1.getGrade();
						}
					}				
		        }

				
				Put put = populatePut(testidInt.toString(),mode,starttime, endtime, filename, sn,
					partnumber,stationname,operatorname, testname, version, quantity, measurelsl,
						measureusl, measurename, measureunit, measurevalue, testendtime, teststarttime,outcome,
						childcolumns, childvalues,parentcolumn, parentvalue);
				putItems.add(put);
				childValueList.add(outcome);
				childColumnList.add(testidInt.toString());
 
			}
			
		}
        Integer testidInt = new Integer(parentid);
		// create row key
		
		// create child column and child value
		String[] childvalues = new String[childValueList.size()];
		String[] childcolumns = new String[childColumnList.size()];
		childValueList.toArray(childvalues);
		childColumnList.toArray(childcolumns);
		String parentcolumn = null;
		String parentvalue = null;
		Put put = populatePut(testidInt.toString(),mode,starttime, endtime, filename, sn,
			partnumber,stationname,operatorname, testname, version, quantity, measurelsl,
				measureusl, measurename, measureunit, measurevalue, testendtime, teststarttime,outcome,
				childcolumns, childvalues,parentcolumn, parentvalue);
		putItems.add(put);

		return putItems;
	}

	private static Put populatePut(String testid, String mode,String starttime, String endtime, String filename, String sn,
			String partnumber,String stationname,String operatorname, String testname, String version, Long quantity, Double measurelsl,
			Double measureusl, String measurename, String measureunit, Double measurevalue,String testendtime, String teststarttime,String outcome,
			String[] childcolumns, String[] childvalues,String parentcolumn, String parentvalue){
		
		byte prefix = (byte) (starttime.hashCode() % number_of_region_servers);
		byte[] rowkey = null;
		if(outcome != null){ 
			rowkey = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(starttime.concat(outcome).concat(stationname).concat(testid)));
		}else{
			rowkey = Bytes.add(Bytes.toBytes(prefix), Bytes.toBytes(starttime.concat(stationname).concat(testid)));			
		}
		
		Put put = new Put(rowkey);
		// put column family -header
		put.add(Bytes.toBytes("h"), Bytes.toBytes("Debug"), Bytes.toBytes(mode));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("Endtime"),
				Bytes.toBytes(endtime));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("Filename"),
				Bytes.toBytes(filename));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("Serialnumber"),
				Bytes.toBytes(sn));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("ProductID"),
				Bytes.toBytes(partnumber));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("StationID"),
				Bytes.toBytes(stationname));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("OperatorID"),
				Bytes.toBytes(operatorname));
		put.add(Bytes.toBytes("h"), Bytes.toBytes("TestID"),
				Bytes.toBytes(testname));
		if (version != null)
			put.add(Bytes.toBytes("h"), Bytes.toBytes("VersionID"),
				Bytes.toBytes(version));
		if (quantity != null)
		put.add(Bytes.toBytes("h"), Bytes.toBytes("Quantity"),
				Bytes.toBytes(quantity));
		
		//put.add(Bytes.toBytes("h"), Bytes.toBytes("Seq"), Bytes.toBytes(""));
		//put.add(Bytes.toBytes("h"), Bytes.toBytes("Code"), Bytes.toBytes(""));

		// put column family --measurement
		if (measurelsl != null)
		put.add(Bytes.toBytes("m"), Bytes.toBytes("LSL"),
				Bytes.toBytes(measurelsl));
		if (measureusl != null)
		put.add(Bytes.toBytes("m"), Bytes.toBytes("USL"),
				Bytes.toBytes(measureusl));
		if (measurename != null)
		put.add(Bytes.toBytes("m"), Bytes.toBytes("MeasureName"),
				Bytes.toBytes(measurename));
		if (measureunit != null)
		put.add(Bytes.toBytes("m"), Bytes.toBytes("MeasureUnit"),
				Bytes.toBytes(measureunit));
		if (measurevalue != null)
		put.add(Bytes.toBytes("m"), Bytes.toBytes("MeasureValue"),
				Bytes.toBytes(measurevalue));

		// put column family --- custom property
		put.add(Bytes.toBytes("cp"), Bytes.toBytes("TestStarttime"),
				Bytes.toBytes(teststarttime));
		if(testendtime != null)
		put.add(Bytes.toBytes("cp"), Bytes.toBytes("TestEndtime"),
				Bytes.toBytes(testendtime));


		// put column family --- parent
		if (parentcolumn != null && parentvalue != null) {
			put.add(Bytes.toBytes("p"), Bytes.toBytes(parentcolumn),
					Bytes.toBytes(parentvalue));
		}

		// put column family --- child
		if (childvalues.length != 0 && childcolumns.length != 0) {
			for(int i = 0; i< childvalues.length; i++){
			put.add(Bytes.toBytes("c"), Bytes.toBytes(childcolumns[i]), Bytes.toBytes(childvalues[i]));
			}
		}
		return put;
	}

	public static HTable createHTable(String filepath) throws IOException,
			JAXBException {
		Configuration conf = HBaseConfiguration.create();

		HTable table = new HTable(conf, "test");
		table.setAutoFlush(false);

		File dir = new File(filepath);

		File[] files = dir.listFiles();

		int testNbr = 1;

		for (File file : files) {

			if (file.isDirectory())
				continue;

			String xml = new String();
			xml = FileUtils.readFileToString(file);
			ArrayList<Put> putLists;
			try {
				putLists = createHPutItems(xml);
				for (Put p : putLists) {
					table.put(p);
					System.out.println("Loaded test " + testNbr++);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		table.flushCommits();
		return table;
	}
	
	public static HTable createHTable(ResultSet rs) throws IOException, JAXBException, SQLException {
		Configuration conf = HBaseConfiguration.create();		
		HTable table = new HTable(conf, "test");
		table.setAutoFlush(false);		
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

			String newxml = xml;

			if (xml.indexOf("urn:unitreport-schema") == -1)
			{

				String sub = new String("xmlns");
				int index = xml.indexOf(sub);
				String f1 = xml.substring(0,index);
				String f2 = xml.substring(index);
				//String newxml = f1.concat(" xmlns=\"urn:unitreport-schema\" ").concat(f2);
				newxml = f1 + " xmlns=\"urn:unitreport-schema\" " + f2;
			}

			count++;
			ArrayList<Put> putLists = null;
			try {
				putLists = createHPutItems(newxml);
			}catch(JAXBException e){
				errcount++;
				System.out.println("errcount = "+errcount+ "\n"+newxml);
				//throw e;
				continue;
			}
			for(Put p : putLists){
				table.put(p);
			}
			if(count%100000==0){
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
	}
	
	public static void main(String[] args) {
		
		
		try {
			jc = JAXBContext.newInstance("net.sigmaquest.supplychain.model");
		} catch (Exception e1) {
			System.out.println(e1.getMessage());
			e1.printStackTrace();
		}

		try {
			u = jc.createUnmarshaller();
			u.setSchema(null);			
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
			rs.close();
			statement.close();
			//HTable table = createHTable("C:\\Users\\pcliu.CAMSTAR\\Desktop\\xml");
		} catch (Exception e3) {
			e3.printStackTrace();
		}

	}

}
