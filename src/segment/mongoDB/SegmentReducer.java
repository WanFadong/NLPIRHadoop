package segment.mongoDB;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.bson.BSONObject;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.hadoop.io.BSONWritable;

public class SegmentReducer extends Reducer<Text, IntWritable, IntWritable, BSONWritable> {
	public static int id = 0;

	@Override
	public void reduce(Text word, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		id++;
		int sum = 0;
		Iterator<IntWritable> iterator = values.iterator();
		while (iterator.hasNext()) {
			sum += iterator.next().get();
		}
		BSONObject wordKey = BasicDBObjectBuilder.start().add("word", word.toString()).add("count", sum).get();
		BSONWritable wordWrite = new BSONWritable(wordKey);
		context.write(new IntWritable(id), wordWrite);
	}
}
