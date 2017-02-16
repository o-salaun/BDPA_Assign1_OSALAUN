package stopwordcount; // put name of package

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex_a_ii extends Configured implements Tool { 
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex_a_ii(), args);
      
      System.exit(res);
   }

   // DRIVER CLASS

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertedIndex_a_ii");

      job.setJarByClass(InvertedIndex_a_ii.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(IntWritable.class);

      job.setMapperClass(Map.class);
      job.setCombinerClass(Reduce.class); // add combiner
      job.setReducerClass(Reduce.class);
      job.setNumReduceTasks(10); // use 10 reducers

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ; ");
      // use semicolon for separating keys and values in the csv output file

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      FileSystem fs = FileSystem.newInstance(getConf());

      if (fs.exists(new Path(args[1]))) {
         fs.delete(new Path(args[1]), true);
      }

      job.waitForCompletion(true);
      
      return 0;
   }
   
   // MAP CLASS
   
   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable ONE = new IntWritable(1);
      private Text word = new Text();

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {

         for (String string: value.toString().split("\\s+")) {
            word.set(string.toLowerCase()); // put words in lowercase, so no differentiation between upper and lower case characters
            context.write(word, ONE);
         }
      }
   }
   
   // REDUCE CLASS
   
   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
      @Override
      public void reduce(Text key, Iterable<IntWritable> values, Context context)
              throws IOException, InterruptedException {

         int sum = 0;

         for (IntWritable val : values) {
            sum = sum + val.get();
         }
         if (sum > 4000){
            // take into account stop words with occurences > 4000
            context.write(key, new IntWritable(sum));
            // include stop words in the output file
         }
      }
   }
}