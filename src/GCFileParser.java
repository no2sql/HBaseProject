import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


public class GCFileParser {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		BufferedReader r = new BufferedReader(new FileReader("C:/Users/kfarmer.CAMSTAR/workspace/HBaseProject/gc-hbase.log"));
		
		String line = null;
		
		float gc = 0;
		float yg = 0;
		float max = 0;
		while ((line = r.readLine()) != null)
		{
			if (line.contains("concurrent"))
				continue;
			if (line.contains("GC[YG"))
			{
				int loc = line.indexOf("[weak refs processing, ");
				float val = Float.parseFloat(line.substring(loc+23, line.indexOf("secs",loc + 23)));
				yg += val;
				if (val > max)
					max = val;
				//System.out.println(line.substring(loc+23, line.indexOf("secs",loc + 23)));
			}
			else
			{
				int loc = line.indexOf("K), ");
				float val = Float.parseFloat(line.substring(loc+4, line.indexOf(" secs",loc + 4)));
				gc += val;
				if (val > max)
					max = val;
				//System.out.println(line.substring(loc+4, line.indexOf(" secs",loc + 4)));
			}
				
		}

		System.out.println(yg);
		System.out.println(gc);
		System.out.println(max);
	}

}
