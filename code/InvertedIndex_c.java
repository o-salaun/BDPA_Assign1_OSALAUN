package stopwordcount; // put name of the package

import java.io.IOException;
import java.util.Arrays;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class InvertedIndex_c extends Configured implements Tool { // put name of the class

   public static enum UNIQUE_WORDS_COUNTER { // add counter for unique words
	   NUMBER_OF_UNIQUE_WORDS,
      };

   
   public static void main(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      int res = ToolRunner.run(new Configuration(), new InvertedIndex_c(), args);
      
      System.exit(res);
   }

   // DRIVER CLASS

   @Override
   public int run(String[] args) throws Exception {
      System.out.println(Arrays.toString(args));
      Job job = new Job(getConf(), "InvertedIndex_c");

      job.setJarByClass(InvertedIndex_c.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);

      job.setMapperClass(Map.class);
      //job.setCombinerClass(Reduce.class); // add combiner
      job.setReducerClass(Reduce.class);

      job.setInputFormatClass(TextInputFormat.class);
      job.setOutputFormatClass(TextOutputFormat.class);
      
      job.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ; "); // added for making output file
      job.setNumReduceTasks(10); // use 10 reducer

      FileInputFormat.addInputPath(job, new Path(args[0]));
      FileOutputFormat.setOutputPath(job, new Path(args[1]));

      FileSystem fs = FileSystem.newInstance(getConf());

      if (fs.exists(new Path(args[1]))) { // deletes and replaces existing folder or output files
         fs.delete(new Path(args[1]), true);
      }

      job.waitForCompletion(true);
      
      return 0;
   }
   
   // MAP CLASS
   
   public static class Map extends Mapper<LongWritable, Text, Text, Text> {
      
      private Text word = new Text(); // define key of mapper output
      private Text filename = new Text(); // define value of mapper output

      @Override
      public void map(LongWritable key, Text value, Context context)
              throws IOException, InterruptedException {
           
            HashSet<String> stopwordslist = new HashSet<String>();
            BufferedReader Reader = new BufferedReader(
                  new FileReader(
                     new File("/home/cloudera/workspace/InvertedIndex_c/StopWords.csv")));

            String pattern;
            while ((pattern = Reader.readLine()) != null) {
               stopwordslist.add(pattern.toLowerCase()); // store stop words set in lower case into a hashset
            }

            String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
            filename = new Text(filenameStr);

            for (String string: value.toString().split("\\s+")) {
               if (!stopwordslist.contains(string.toLowerCase())) {
                  word.set(string.toLowerCase()); // set all non stop words to lower case
               }
            }

            context.write(word, filename); // generate key-value pairs of mapper output
   }
}
   
   // REDUCE CLASS
   
   public static class Reduce extends Reducer<Text, Text, Text, Text> {
      @Override
      public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

         HashSet<String> set = new HashSet<String>();

         for (Text value : values) {
            set.add(value.toString());
         }

         if (set.size() == 1) {

            context.getCounter(UNIQUE_WORDS_COUNTER.NUMBER_OF_UNIQUE_WORDS).increment(1);
            // increases the counter by one for each new unique word

            StringBuilder builder = new StringBuilder();
            // in the value of the reducer output, the filenames are separated by commas and spaces
            String separator = "";
            for (String value : set) {
               builder.append(separator);
               separator = ", ";
               builder.append(value);
            }

          context.write(key, new Text(builder.toString())); // generate key-value pairs of reducer output

         }
      }
   }
}