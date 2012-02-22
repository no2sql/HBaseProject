import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;


public class Test2 {

	/**
	 * @param args
	 */
	private static SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss Z");
	private static GregorianCalendar cal = new GregorianCalendar();
	public static void main(String[] args) throws ParseException, IOException {
		GregorianCalendar cal = new GregorianCalendar();
		  
		String testString = "2011-05-29T21:47:00 +0800";
		Date testDate = df.parse(testString);
		GregorianCalendar testCal = new GregorianCalendar();
		testCal.setTime(testDate);
		
		
		  String dateStr = "2011-01-27T00:00:00 +0000";
		  	  
		  Date date;
		  date = df.parse(dateStr); cal.setTime(date); 	
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-01-29T00:00:00 +0000";		  
		  date = df.parse(dateStr);		  cal.setTime(date); 		  
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		 
		  dateStr = "2011-01-31T00:00:00 +0000";		  
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		 
		  dateStr = "2011-02-02T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-02-06T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-02-08T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  			  
		  dateStr = "2011-04-30T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-03T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-07T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-13T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-19T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-21T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-23T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-25T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  			  
		  dateStr = "2011-05-27T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-29T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));			  
		  
		  for (byte b : (Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))))
		  {
			  System.out.print(b + " ");
		  }
		  System.out.println();
		  
		  System.out.println("Test...");
		  for (byte b : (Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(testCal.getTimeInMillis())))))
		  {
			  System.out.print(b + " ");
		  }
		  System.out.println("End test...");
		  
		  
		  
		  dateStr = "2011-06-01T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));		
		  for (byte b : (Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))))
		  {
			  System.out.print(b + " ");
		  }
		  System.out.println();
		  
		  
		  dateStr = "2011-06-03T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-05T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-06T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-08T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-10T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)0}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  
		  
		  dateStr = "2011-01-27T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-05-02T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-03T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
		  dateStr = "2011-06-06T00:00:00 +0000";
		  date = df.parse(dateStr); cal.setTime(date); 
		  System.out.println(dateStr + " " + Bytes.toStringBinary(Bytes.add(Bytes.toBytes('P'),Bytes.add(new byte[]{(byte)1}, Bytes.toBytes(cal.getTimeInMillis())))));
		  
	}

}
