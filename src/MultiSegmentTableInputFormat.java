import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.mapreduce.TableInputFormatBase;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;


public class MultiSegmentTableInputFormat extends TableInputFormatBase {

	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException {
		// TODO Auto-generated method stub
		return super.getSplits(context);
	}

	
	
}
