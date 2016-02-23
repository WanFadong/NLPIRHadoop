package segment.mongoDB;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.bson.BSONObject;

import com.mongodb.BasicDBObject;

public class SegmentMapper extends Mapper<Object, BSONObject, Text, IntWritable> {
	private CLibrary lib;
	private final IntWritable one = new IntWritable(1);
	private static int count = 0;
	private static int count2 = 0;

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
	public void map(Object key, BSONObject value, Context context) throws IOException, InterruptedException {
		String verse = (String) value.get("verse");// 获取诗句
		// List<BasicDBObject> verses = (List<BasicDBObject>)
		// value.get("source");// 获取诗句
		// String verse = verses.get(0).getString("content");
		// System.out.println(verse);
		count++;
		if (count % 100 == 0) {
			System.out.println(count);
		}
		// if (count >= 20001&&count<30001) {
		String result = lib.NLPIR_ParagraphProcess(verse.toString(), 0);
		String[] words = result.split(" ");
		if (count == 12905) {
			System.out.println(result + "/n" + words.length + words[0]);
		}
		for (int i = 0; i < 1; i++) {
			String word = words[i];
			// if(word.equals("，“) ||word.equals("。") )
			context.write(new Text(word), one);
		}
		// }

		// context.write(new Text(verse), one);
	}
}
