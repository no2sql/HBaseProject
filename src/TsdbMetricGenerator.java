import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.commons.io.FileUtils;


public class TsdbMetricGenerator {
	
	public static final int NUM_DATA_POINTS = 10000000;
	public static final int NUM_DAYS = 1;
	public static final int NUM_METRICS = 100;
	public static final int NUM_TAGS = 5;
	public static final int NUM_TAG_VALUES_PER = 100;
	
	public static final List<Long> timestamps = new ArrayList<Long>(NUM_DATA_POINTS);
	
	private static Random random = new Random();
	
	static
	{
		for (int i = 0; i < NUM_DATA_POINTS; i++)
		{
			timestamps.add(System.currentTimeMillis()/1000 - random.nextInt(86400*NUM_DAYS));
		}
		Collections.sort(timestamps);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		
		File file = new File("./metric_data.txt");
		
		BufferedWriter writer = new BufferedWriter(new FileWriter(file));
		
		for (int i = 0; i < NUM_DATA_POINTS; i++)
		{		
			StringBuffer line = new StringBuffer();
			line.append("metric");
			line.append(random.nextInt(NUM_METRICS)+1);
			line.append(" ");
			line.append(timestamps.get(i));
			line.append(" ");
			line.append(random.nextInt(100));
			line.append(" ");
			for (int tag = 1; tag <= NUM_TAGS; tag++)
			{
				line.append("tag");
				line.append(tag);
				line.append("=");
				line.append("tagval" + random.nextInt(NUM_TAG_VALUES_PER)+1);
				line.append(" ");
			}
			line.append("\n");
			if (i % 100000 == 0)
				System.out.println("Line " + i);
			writer.write(line.toString());
		}
		writer.flush();
		writer.close();

	}

}
