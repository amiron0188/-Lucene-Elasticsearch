import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import java.io.IOException;

public class EsToHDFS {
	public static class MyMapper extends Mapper<Writable, Writable, NullWritable, Text> {
		@Override
		protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
			Text text = new Text();
			text.set(value.toString());
			context.write(NullWritable.get(), text);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("es.nodes", "localhost:9200");
		configuration.set("es.resource", "blog/csdn");
		configuration.set("es.output.json", "true");
		Job job = Job.getInstance(configuration, "hadoop es write test");
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(EsInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/work /blog_csdn"));
		job.waitForCompletion(true);
	}
}
