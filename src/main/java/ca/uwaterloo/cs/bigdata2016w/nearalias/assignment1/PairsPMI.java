package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment1;

import java.io.IOException;
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
import org.apache.hadoop.fs.FileStatus;
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

import tl.lin.data.pair.PairOfStrings;

public class PairsPMI extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(PairsPMI.class);

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

  private static class SecondMapper extends Mapper<LongWritable, Text, PairOfStrings, IntWritable> {
    private static final PairOfStrings PAIR = new PairOfStrings();
    private static final IntWritable ONE = new IntWritable(1);

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
        for (int k = 0; k < words.length; k++) {
          if (i != k) {
            PAIR.set(words[i], words[k]);
            context.write(PAIR, ONE);
          }
        }
      }
    }
  }

  private static class MyCombiner extends Reducer<PairOfStrings, IntWritable, PairOfStrings, IntWritable> {
    private final static IntWritable VALUE = new IntWritable();

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int xy = 0;
      while (iter.hasNext()) {
        xy += iter.next().get();
      }
      VALUE.set(xy);
      context.write(key, VALUE);
    }
  }

  private static class SecondReducer extends Reducer<PairOfStrings, IntWritable, PairOfStrings, FloatWritable> {
    private final static FloatWritable PMI = new FloatWritable();
    private static HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    private static int TOTAL_NUM_LINES = 0;

    @Override
    public void setup(Context context) {
      try {
LOG.info("-------------- entering trycatch --------------");
        FileSystem fs = FileSystem.get(context.getConfiguration());
LOG.info("-------------- creatd filesystem --------------");
        FileStatus[] status = fs.listStatus(new Path("ideservechallenjourgg"));
LOG.info("-------------- got file statuses --------------");
        for (int i = 0; i < status.length; i++) {
          Path file = status[i].getPath();
LOG.info("-------------- opening: "+file.getName()+" --------------");
          if (!(file.getName().startsWith("part-r"))) continue;
          BufferedReader in = new BufferedReader(new InputStreamReader(fs.open(file), "UTF-8"));
LOG.info("-------------- buffered reader is good, file opened --------------");
          String line = null;
          while ((line = in.readLine()) != null) {
            String[] kv = line.split("\\s+");
            dataMap.put(kv[0], Integer.parseInt(kv[1]));
          }
          in.close();
        }
LOG.info("-------------- size:  "+ dataMap.size()+"-----------");
LOG.info("-------------- exiting trycatch --------------");
      } catch (Exception e) {
LOG.info("-------------- errored out --------------");
      }

LOG.info("-------------- finish size:  "+ dataMap.size()+"-----------");
      TOTAL_NUM_LINES = dataMap.get("*");
    }

    @Override
    public void reduce(PairOfStrings key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
      Iterator<IntWritable> iter = values.iterator();
      int xy = 0;
      while (iter.hasNext()) {
        xy += iter.next().get();
      }
      if (xy < 10) return;
      int x = dataMap.get(key.getLeftElement());
      int y = dataMap.get(key.getRightElement());
      float log = (float) Math.log10((double) (xy * TOTAL_NUM_LINES) / (double) (x * y));
      PMI.set(log);
      context.write(key, PMI);
    }
  }

  private static class MyPartitioner extends Partitioner<PairOfStrings, IntWritable> {
    @Override
    public int getPartition(PairOfStrings key, IntWritable value, int numReduceTasks) {
      return (key.getLeftElement().hashCode() & Integer.MAX_VALUE) % numReduceTasks;
    }
  }

  public PairsPMI() {}

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

    LOG.info("Tool: " + PairsPMI.class.getSimpleName());
    LOG.info(" - input path: " + args.input);
    LOG.info(" - output path: " + args.output);
    LOG.info(" - number of reducers: " + args.numReducers);

    Job firstJob = Job.getInstance(getConf());
    firstJob.getConfiguration().setInt("mapred.max.split.size", 1024 * 1024 * 64);
    firstJob.getConfiguration().set("mapreduce.map.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.map.java.opts", "-Xmx3072m");
    firstJob.getConfiguration().set("mapreduce.reduce.memory.mb", "3072");
    firstJob.getConfiguration().set("mapreduce.reduce.java.opts", "-Xmx3072m");

    firstJob.setJobName("First"+PairsPMI.class.getSimpleName());
    firstJob.setJarByClass(PairsPMI.class);
    firstJob.setNumReduceTasks(args.numReducers);
    FileInputFormat.setInputPaths(firstJob, new Path(args.input));
    FileOutputFormat.setOutputPath(firstJob, new Path("ideservechallenjourgg"));
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

    secondJob.setJobName("Second"+PairsPMI.class.getSimpleName());
    secondJob.setJarByClass(PairsPMI.class);
    secondJob.setNumReduceTasks(args.numReducers);
    FileInputFormat.setInputPaths(secondJob, new Path(args.input));
    FileOutputFormat.setOutputPath(secondJob, new Path(args.output));
    secondJob.setMapOutputKeyClass(PairOfStrings.class);
    secondJob.setMapOutputValueClass(IntWritable.class);
    secondJob.setOutputKeyClass(PairOfStrings.class);
    secondJob.setOutputValueClass(FloatWritable.class);
    secondJob.setOutputFormatClass(TextOutputFormat.class);
    secondJob.setMapperClass(SecondMapper.class);
    secondJob.setCombinerClass(MyCombiner.class);
    secondJob.setReducerClass(SecondReducer.class);
    secondJob.setPartitionerClass(MyPartitioner.class);

    // Delete the output directory if it exists already.
    Path outputDir = new Path(args.output);
    FileSystem.get(getConf()).delete(outputDir, true);

    long startTime = System.currentTimeMillis();
    firstJob.waitForCompletion(true);
    LOG.info("First Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    startTime = System.currentTimeMillis();
    secondJob.waitForCompletion(true);
    LOG.info("Second Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    FileSystem.get(getConf()).delete(new Path("ideservechallenjourgg"), true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    ToolRunner.run(new PairsPMI(), args);
  }

}
