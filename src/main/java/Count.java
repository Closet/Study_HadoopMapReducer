import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.WordCount;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;

public class Count {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(Count.class);
        job.setMapperClass(HelloCountMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class HelloCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static Set<String> capture = new HashSet<>();
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public HelloCountMapper() {
            capture.add("hello");
        }

        public void setFilterString(Iterator<String> candidates) {
            while (candidates.hasNext()) {
                capture.add(candidates.next());
            }
        }


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String token =itr.nextToken();
                if (isEqualFilteredText(token))
                {
                    word.set(token);
                    context.write(word, one);
                }
            }
        }

        private boolean isEqualFilteredText(String text) {
            return capture.size()==0||capture.contains(text);
        }
    }
}
