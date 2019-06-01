import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.elasticsearch.hadoop.mr.EsOutputFormat;
import java.io.IOException;

public class HdfsToES {
	public static class MyMapper extends Mapper<Object, Text, NullWritable, BytesWritable> {
		public void map(Object key, Text value, Mapper<Object, Text, NullWritable, BytesWritable>.Context context)
				throws IOException, InterruptedException {
			byte[] line = value.toString().trim().getBytes();
			BytesWritable blog = new BytesWritable(line);
			context.write(NullWritable.get(), blog);
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		conf.setBoolean("mapred.map.tasks.speculative.execution", false);
		conf.setBoolean("mapred.reduce.tasks.speculative.execution", false);
		conf.set("es.nodes", "192.168.1.111:9200");
		conf.set("es.resource", "blog/csdn");
		conf.set("es.mapping.id", "id");
		conf.set("es.input.json", "yes");

		Job job = Job.getInstance(conf, "hadoop es write test");
		job.setMapperClass(HdfsToES.MyMapper.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(EsOutputFormat.class);
		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(BytesWritable.class);

		FileInputFormat.setInputPaths(job, new Path("hdfs://localhost:9000//work/blog.json"));
		job.waitForCompletion(true);
	}
}
