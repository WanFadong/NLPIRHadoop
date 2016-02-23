package segment.HDFS;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

public class SegmentMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private CLibrary lib;
	private final IntWritable one = new IntWritable(1);
	private static int count = 0;

	@Override
	public void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		lib = SegmentHelp.getLib();
	}

	@Override
	public void cleanup(Context context) throws IOException, InterruptedException {
		super.cleanup(context);
		SegmentHelp.destroyLib();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		count++;
		if (count % 5000 == 0) {
			System.out.println(count);
		}
		String result = lib.NLPIR_ParagraphProcess(value.toString(), 0);
		String[] words = result.split(" ");
		for (int i = 0; i < words.length; i++) {
			String word = words[i];
			context.write(new Text(word), one);
		}
	}
}
