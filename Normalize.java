import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //movieA:movieB \t relation
            //collect the relationship list for movieA
            String[] splited = value.toString().trim().split("\t");
            String[] movies = splited[0].split(":");
            String movieA = movies[0];
            String movieB = movies[1];
            int relation = Integer.parseInt(splited[1]);
            context.write(new Text(movieA), new Text(movieB + ":" + relation));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //key = movieA, value=<movieB:relation, movieC:relation...>
            //normalize each unit of co-occurrence matrix
            int sum = 0;
            Iterator<Text> it = values.iterator();
            Map<String, Integer> map = new HashMap<String, Integer>();
            while (it.hasNext()) {
                String item = it.next().toString();
                String movieId = item.split(":")[0];
                int relation = Integer.parseInt(item.split(":")[1]);
                sum += relation;
                map.put(movieId, relation);
            }
            for (String movieId : map.keySet()) {
                double rating = (double)map.get(movieId)/(double)sum;
                context.write(new Text(movieId), new Text(key.toString() + "=" + rating));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
