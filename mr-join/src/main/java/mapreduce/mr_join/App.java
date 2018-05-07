package mapreduce.mr_join;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class App {
	public static class RecordPartitioner extends Partitioner<Text, Text> {
		@Override
		public int getPartition(Text key, Text val, int numReduceTasks) {
			String joinKey = key.toString();
			joinKey = joinKey.substring(0, joinKey.length() - 1);

			return joinKey.hashCode() % numReduceTasks;
		}
	}

	public static class RecordMapper extends Mapper<LongWritable, Text, Text, Text> {
		private Text keyOutput = new Text();
		private Text valOutput = new Text();

		private enum RecordType {
			TypeA, TypeB
		};

		private RecordType recordType;

		@Override
		protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			if (((FileSplit) context.getInputSplit()).getPath().getParent().toString().contains("TypeA")) {
				recordType = RecordType.TypeA;
			} else {
				recordType = RecordType.TypeB;
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] recordElems = value.toString().split("\\|");

			if (recordType == RecordType.TypeA) {
				keyOutput.set(recordElems[0] + "A");
			} else {
				keyOutput.set(recordElems[0] + "B");
			}
			valOutput.set(recordElems[1]);
			context.write(keyOutput, valOutput);
		}
	}

	public static class RecordReducer extends Reducer<Text, Text, Text, Text> {
		private enum RecordType {
			TypeA, TypeB
		};

		private String currentKey = null;
		private List<String> aRecordVals = new ArrayList<String>();
		private Text keyOutput = new Text();
		private Text valOutput = new Text();

		private FSDataOutputStream log;

		@Override
		protected void setup(Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
			FileSystem fs = FileSystem.get(new Configuration());
			log = fs.create(new Path("/mr-log/join.log"));
		}

		@Override
		protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			log.close();
		}

		@Override
		protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String keyString = key.toString();
			RecordType recordType = keyString.substring(keyString.length() - 1).equals("A") ? RecordType.TypeA
					: RecordType.TypeB;
			String joinKey = keyString.substring(0, keyString.length() - 1);

			if (currentKey == null) {
				currentKey = joinKey;
			}

			if (currentKey.equals(joinKey) == false) {
				aRecordVals = new ArrayList<String>();
				currentKey = joinKey;
			}

			if (recordType == RecordType.TypeA) {
				for (Text value : values) {
					aRecordVals.add(value.toString());
					log.write(joinKey.getBytes());
					log.write("|".getBytes());
					log.write(value.toString().getBytes());
					log.write("\n".getBytes());
				}
				context.progress();
			} else {
				for (Text value : values) {
					for (String aRecordVal : aRecordVals) {
						keyOutput.set(currentKey);
						valOutput.set(aRecordVal + "|" + value.toString());
						context.write(keyOutput, valOutput);

						log.write(joinKey.getBytes());
						log.write("|".getBytes());
						log.write(value.toString().getBytes());
						log.write("\n".getBytes());
					}
				}
			}
		}
	}

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "wordcount");
		job.setJarByClass(App.class);

		job.setPartitionerClass(RecordPartitioner.class);
		job.setMapperClass(RecordMapper.class);
		job.setReducerClass(RecordReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
