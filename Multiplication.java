import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation
			//pass data to reducer
			String line = value.toString().trim();
			String movieB = line.split("\t")[0];
			String movieARelation = line.split("\t")[1];
			context.write(new Text(movieB), new Text(movieARelation));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: user,movie,rating
			//pass data to reducer
			String[] splited = value.toString().trim().split(",");
			String userId = splited[0];
			String movieId = splited[1];
			String rating = splited[2];
			context.write(new Text(movieId), new Text(userId + ":" + rating));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
			Map<String, Double> movieRelation = new HashMap<String, Double>();
			Map<String, Double> userRating = new HashMap<String, Double>();
			Iterator<Text> it = values.iterator();
			while (it.hasNext()) {
				String line = it.next().toString();
				if (line.contains("=")) {
					String[] splited = line.split("=");
					movieRelation.put(splited[0], Double.parseDouble(splited[1]));
				}
				else if (line.contains(":")) {
					String[] splited = line.split(":");
					userRating.put(splited[0], Double.parseDouble(splited[1]));
				}
			}
			for (String user: userRating.keySet()) {
				for (String movie: movieRelation.keySet()) {
					String outputkey = user + ":" + movie;
					double rating = userRating.get(user) * movieRelation.get(movie);
					context.write(new Text(outputkey), new DoubleWritable(rating));
				}
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
