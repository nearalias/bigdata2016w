package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment1;

import java.io.IOException;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.ParserProperties;

import tl.lin.data.map.HMapStIW;
import tl.lin.data.map.HMapStFW;

public class StripesPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(StripesPMI.class);

  private static class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static Text WORD = new Text();
    private final static IntWritable ONE = new IntWritable(1);

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int count = 0;
      Set<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        count ++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (count >= 100) break;
      }
      String[] words = new String[set.size()];
      words = set.toArray(words);

      for (int i = 0; i < words.length; i++) {
        WORD.set(words[i]);
        context.write(WORD, ONE);
      }
      WORD.set("*");
      context.write(WORD, ONE);  // counter for total number of lines
    }
  }

  private static class FirstReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private static final IntWritable SUM = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      int sum = 0;
      Iterator<IntWritable> iter = values.iterator();
      while (iter.hasNext()) {
        sum += iter.next().get();
      }
      SUM.set(sum);
      context.write(key, SUM);
    }
  }

  private static class SecondMapper extends Mapper<LongWritable, Text, Text, HMapStIW> {
    private static final Text KEY = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String line = ((Text) value).toString();
      StringTokenizer itr = new StringTokenizer(line);

      int count = 0;
      Set<String> set = new HashSet<String>();
      while (itr.hasMoreTokens()) {
        count ++;
        String w = itr.nextToken().toLowerCase().replaceAll("(^[^a-z]+|[^a-z]+$)", "");
        if (w.length() == 0) continue;
        set.add(w);
        if (count >= 100) break;
      }
      if (set.size() < 2) return;
      String[] words = new String[set.size()];
      words = set.toArray(words);

      for (int i = 0; i < words.length; i++) {
        HMapStIW stripe = new HMapStIW();
        for (int k = 0; k < words.length; k++) {
          if (i != k) {
            stripe.put(words[k], 1);
          }
        }
        KEY.set(words[i]);
        context.write(KEY, stripe);
      }
    }
  }

  private static class MyCombiner extends Reducer<Text, HMapStIW, Text, HMapStIW> {
    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context) throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW combinedMap = new HMapStIW();
      while (iter.hasNext()) {
        combinedMap.plus(iter.next());
      }
      context.write(key, combinedMap);
    }
  }

  private static class SecondReducer extends Reducer<Text, HMapStIW, Text, HMapStFW> {
    private static HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    private static int TOTAL_NUM_LINES = 0;

    @Override
    public void setup(Context context) {
      try {
        File folder = new File("ggideservechallenjourgg");
        for (File file : folder.listFiles()) {
          if (!(file.getName().startsWith("part-r"))) continue;
          FileInputStream fis = new FileInputStream(file);
          BufferedReader in = new BufferedReader(new InputStreamReader(fis, "UTF-8"));
          String line = null;
          while ((line = in.readLine()) != null) {
            String[] kv = line.split("\\s+");
            dataMap.put(kv[0], Integer.parseInt(kv[1]));
          }
          in.close();
        }
      } catch (Exception e) {
      }
      TOTAL_NUM_LINES = dataMap.get("*");
    }

    @Override
    public void reduce(Text key, Iterable<HMapStIW> values, Context context) throws IOException, InterruptedException {
      Iterator<HMapStIW> iter = values.iterator();
      HMapStIW combinedMap = new HMapStIW();
      while (iter.hasNext()) {
        combinedMap.plus(iter.next());
      }
      int x = dataMap.get(((Text) key).toString());
      HMapStFW PMIMap = new HMapStFW();
      for (String word : combinedMap.keySet()) {
        int y = dataMap.get(word);
        int xy = combinedMap.get(word);
        if (xy < 10) continue;
        float log = (float) Math.log10((double) (xy * TOTAL_NUM_LINES) / (double) (x * y));
        PMIMap.put(word, log);
      }
      if (PMIMap.size() == 0) return;

      context.write(key, PMIMap);
    }
  }

  public StripesPMI() {}

  public static class Args {
    @Option(name = "-input", metaVar = "[path]", required = true, usage = "input path")
    public String input;

    @Option(name = "-output", metaVar = "[path]", required = true, usage = "output path")
    public String output;

    @Option(name = "-reducers", metaVar = "[num]", required = false, usage = "number of reducers")
    public int numReducers = 1;
  }

  /**
   * Runs this tool.
   */
  public int run(String[] argv) throws Exception {
    Args args = new Args();
    CmdLineParser parser = new CmdLineParser(args, ParserProperties.defaults().withUsageWidth(100));

    try {
      parser.parseArgument(argv);
    } catch (CmdLineException e) {
      System.err.println(e.getMessage());
      parser.printUsage(System.err);
      return -1;
    }

    LOG.info("Tool: " + StripesPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job firstJob = Job.getInstance(getConf());
    firstJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    firstJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    firstJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    firstJob.setJobName("First"+StripesPMI.class.getSimpleName());
    firstJob.setJarByClass(StripesPMI.class);
    firstJob.setNumReduceTasks(args.numReducers);
    FileInputFormat.setInputPaths(firstJob, new Path(args.input));
    FileOutputFormat.setOutputPath(firstJob, new Path("ggideservechallenjourgg"));
    firstJob.setMapOutputKeyClass(Text.class);
    firstJob.setMapOutputValueClass(IntWritable.class);
    firstJob.setOutputKeyClass(Text.class);
    firstJob.setOutputValueClass(IntWritable.class);
    firstJob.setOutputFormatClass(TextOutputFormat.class);
    firstJob.setMapperClass(FirstMapper.class);
    firstJob.setCombinerClass(FirstReducer.class);
    firstJob.setReducerClass(FirstReducer.class);

    Job secondJob = Job.getInstance(getConf());
    secondJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    secondJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    secondJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    secondJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    secondJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    secondJob.setJobName("Second"+StripesPMI.class.getSimpleName());
    secondJob.setJarByClass(StripesPMI.class);
    secondJob.setNumReduceTasks(args.numReducers);
    FileInputFormat.setInputPaths(secondJob, new Path(args.input));
    FileOutputFormat.setOutputPath(secondJob, new Path(args.output));
    secondJob.setMapOutputKeyClass(Text.class);
    secondJob.setMapOutputValueClass(HMapStIW.class);
    secondJob.setOutputKeyClass(Text.class);
    secondJob.setOutputValueClass(HMapStFW.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);
    secondJob.setMapperClass(SecondMapper.class);
    secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(SecondReducer.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    LOG.info("First Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Second Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    FileSystem.get(getConf()).delete(new Path("ggideservechallenjourgg"), true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new StripesPMI(), args);
  }

}
