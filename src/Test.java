import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import com.cloudera.cmf.event.shaded.com.google.common.base.Charsets;



public class Test {

	/**
	 * @param args
	 */

	
	private static final Charset CHARSET = Charset.forName("ISO-8859-1");
	
	public static void main(String[] args) throws ParseException, IOException {
		
		
		/*
		for (long i = Long.MAX_VALUE; i >=Long.MAX_VALUE - 1000; i--)
		{
			System.out.println(i + " " + CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(i))).toString().length());
		}*/
		
		
		
		
		
		SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		GregorianCalendar cal = new GregorianCalendar();	
		GregorianCalendar cal2 = new GregorianCalendar();
		cal.setTime(df.parse("2011-01-01 00:00:00"));
		cal2.setTime(df.parse("2011-01-05 00:00:00"));
		
		String patternString = "";
		int i = 0;
		
		//System.out.println("size = " +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().length());
		
		//System.out.println("long is " + cal.getTimeInMillis());
		
		for (i = 0; i < CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().length(); i++)
		{
			if (CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(i) == CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal2.getTimeInMillis()))).toString().charAt(i))
			{
				patternString += CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(i) + "{1}";
				System.out.println("CHAR: " + (int) CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(i));
				//System.out.println(patternString);
			}
			else {
				break;//System.out.println("NO CHAR!!!!!!!!!!");
			}
		}
		
		patternString += "[" +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(i) + "-" +  CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal2.getTimeInMillis()))).toString().charAt(i) + "]{1}";
		
		System.out.println("Pattern = " + patternString);
		Pattern pattern = Pattern.compile(patternString);
  
		for (int day = 1; day < 32; day++)
		{
			String date = "2011-01-" + (day < 10 ? "0" : "") + day + " 00:00:00";
			cal.setTime(df.parse(date));		
			//System.out.println("long is " + cal.getTimeInMillis());
			String dateByteStr = CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString();
			
			for (int j = 0; j < 5; j++)
			{
				//System.out.println("CHAR: " + (int) CHARSET.decode(ByteBuffer.wrap(Bytes.toBytes(cal.getTimeInMillis()))).toString().charAt(j));
			}
			
			System.out.print(date + " " + dateByteStr.length() + " " +  dateByteStr);
			Matcher matcher = pattern.matcher(dateByteStr);
			if (matcher.find())
			{
				System.out.println(" MATCHES");
			}
			else
			{
				System.out.println(" DOESN'T MATCH");
			}
		}
		
	}

}
