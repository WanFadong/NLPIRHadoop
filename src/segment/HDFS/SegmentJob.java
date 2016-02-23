package segment.HDFS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.mongodb.hadoop.MongoInputFormat;
import com.mongodb.hadoop.MongoOutputFormat;
import com.mongodb.hadoop.io.BSONWritable;
import com.mongodb.hadoop.util.MongoConfigUtil;

public class SegmentJob {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		// MongoConfigUtil.setInputURI(conf,
		// "mongodb://127.0.0.1:27017/allusion.poetry");
		// MongoConfigUtil.setInputURI(conf,
		// "mongodb://127.0.0.1:27017/allusion.diangu");
		//MongoConfigUtil.setOutputURI(conf, "mongodb://127.0.0.1:27017/allusion.segment");
		// MongoConfigUtil.setOutputURI(conf,
		// "mongodb://127.0.0.1:27017/test.segment");

		Job job = new Job(conf, "segment");
		String[] ioPath = new String[] { "input", "output" };

		 // 删除输出目录
		 Path outputPath = new Path(ioPath[1]);
		 outputPath.getFileSystem(conf).delete(outputPath, true);

		job.setJarByClass(SegmentJob.class);
		job.setMapperClass(SegmentMapper.class);
		job.setReducerClass(SegmentReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// job.setInputFormatClass(MongoInputFormat.class);
		// job.setOutputFormatClass(MongoOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(ioPath[0]));
		 FileOutputFormat.setOutputPath(job, new Path(ioPath[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
