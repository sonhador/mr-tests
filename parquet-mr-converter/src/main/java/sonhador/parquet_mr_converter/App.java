package sonhador.parquet_mr_converter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

public class App {
	private static final MessageType SCHEMA = MessageTypeParser.parseMessageType(
		      "message Line {\n" +
		      "  required binary col1 (UTF8);\n" +
		      "}");
	
	private static GroupFactory groupFactory = new SimpleGroupFactory(SCHEMA);
	
	public static class LineMapper extends Mapper<LongWritable, Text, Void, Group> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Void, Group>.Context context)
				throws IOException, InterruptedException {
			Group group = groupFactory.newGroup()
					.append("col1", value.toString());
			
			context.write(null, group);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "parquet-converter");
		job.setJarByClass(App.class);
		
		job.setMapperClass(LineMapper.class);
		job.setNumReduceTasks(0);
		
		job.setOutputFormatClass(ExampleOutputFormat.class);
		ExampleOutputFormat.setSchema(job, SCHEMA);
				
		job.setOutputKeyClass(Void.class);
		job.setOutputValueClass(Group.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
