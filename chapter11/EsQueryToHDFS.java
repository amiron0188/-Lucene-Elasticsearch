import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.elasticsearch.hadoop.mr.EsInputFormat;
import java.io.IOException;

public class EsQueryToHDFS {
	public static class MyMapper extends Mapper<Writable, Writable, Text, Text> {
		@Override
		protected void map(Writable key, Writable value, Context context) throws IOException, InterruptedException {
			context.write(new Text(key.toString()), new Text(value.toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration configuration = new Configuration();
		configuration.set("es.nodes", "localhost:9200");
		configuration.set("es.resource", "blog/csdn");
		configuration.set("es.output.json", "true");
		configuration.set("es.query", "?q=title:git");
		Job job = Job.getInstance(configuration, "query es to HDFS");
		job.setMapperClass(MyMapper.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setInputFormatClass(EsInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/work/es_query_to_HDFS"));
		job.waitForCompletion(true);
	}
}
