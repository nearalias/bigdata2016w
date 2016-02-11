package ca.uwaterloo.cs.bigdata2016w.nearalias.assignment4;

import java.io.IOException;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import tl.lin.data.array.ArrayListOfIntsWritable;
import tl.lin.data.map.HMapIF;
import tl.lin.data.map.MapIF;

import com.google.common.base.Preconditions;

/**
 * <p>
 * The starting and ending iterations will correspond to paths
 * <code>/base/path/iterXXXX</code> and <code>/base/path/iterYYYY</code>. As a
 * example, if you specify 0 and 10 as the starting and ending iterations, the
 * driver program will start with the graph structure stored at
 * <code>/base/path/iter0000</code>; final results will be stored at
 * <code>/base/path/iter0010</code>.
 * </p>
 *
 * @see RunPageRankSchimmy
 * @author Jimmy Lin
 * @author Michael Schatz
 */
public class RunPersonalizedPageRankBasic extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(RunPersonalizedPageRankBasic.class);

  private static enum PageRank {
    nodes, edges, massMessages, massMessagesSaved, massMessagesReceived, missingStructure
  };

  private static class MapClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final IntWritable neighbor = new IntWritable();  // The neighbor to which we're sending messages.
    private static final PageRankNode intermediateMass = new PageRankNode();  // Contents of the messages: partial PageRank mass.
    private static final PageRankNode intermediateStructure = new PageRankNode(); // For passing along node structure.

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException {
      // Pass along node structure.
      intermediateStructure.setNodeId(node.getNodeId());
      intermediateStructure.setType(PageRankNode.Type.Structure);
      intermediateStructure.setAdjacencyList(node.getAdjacenyList());

      context.write(nid, intermediateStructure);

      int massMessages = 0;

      // Distribute PageRank mass to neighbors (along outgoing edges).
      if (node.getAdjacenyList().size() > 0) {
        // Each neighbor gets an equal share of PageRank mass.
        ArrayListOfIntsWritable list = node.getAdjacenyList();
        float[] pageRanks = node.getPageRanks();
        float[] masses = new float[pageRanks.length];
        for (int i = 0; i < pageRanks.length; i++) {
          masses[i] = pageRanks[i] - (float) StrictMath.log(list.size());
        }

        context.getCounter(PageRank.edges).increment(list.size());

        // Iterate over neighbors.
        for (int i = 0; i < list.size(); i++) {
          neighbor.set(list.get(i));
          intermediateMass.setNodeId(list.get(i));
          intermediateMass.setType(PageRankNode.Type.Mass);
          intermediateMass.setPageRanks(masses);

          // Emit messages with PageRank mass to neighbors.
          context.write(neighbor, intermediateMass);
          massMessages++;
        }
      }

      // Bookkeeping.
      context.getCounter(PageRank.nodes).increment(1);
      context.getCounter(PageRank.massMessages).increment(massMessages);
    }
  }

  // Combiner: sums partial PageRank contributions and passes node structure along.
  private static class CombineClass extends Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private static final PageRankNode intermediateMass = new PageRankNode();

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> values, Context context) throws IOException, InterruptedException {
      int massMessages = 0;

      int numOfSources = context.getConfiguration().getInt("NumOfSources", 0);
      float[] masses = new float[numOfSources];
      for (int i = 0; i < numOfSources; i++) {
        masses[i] = Float.NEGATIVE_INFINITY;  // Remember, PageRank mass is stored as a log prob.
      }
      for (PageRankNode n : values) {
        if (n.getType() == PageRankNode.Type.Structure) {
          // Simply pass along node structure.
          context.write(nid, n);
        } else {
          // Accumulate PageRank mass contributions.
          float[] pageRanks = n.getPageRanks();
          for (int i = 0; i < numOfSources; i++) {
            masses[i] = sumLogProbs(masses[i], pageRanks[i]);
          }
          massMessages++;
        }
      }

      // Emit aggregated results.
      if (massMessages > 0) {
        intermediateMass.setNodeId(nid.get());
        intermediateMass.setType(PageRankNode.Type.Mass);
        intermediateMass.setPageRanks(masses);

        context.write(nid, intermediateMass);
      }
    }
  }

  // Reduce: sums incoming PageRank contributions, rewrite graph structure.
  private static class ReduceClass extends Reducer<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    //private float totalMass = Float.NEGATIVE_INFINITY;  // For keeping track of PageRank mass encountered, so we can compute missing PageRank mass lost through dangling nodes.
    private int numOfSources;
    private float[] totalMasses;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      numOfSources = conf.getInt("NumOfSources", 0);
      totalMasses = new float[numOfSources];
      for (int i = 0; i < numOfSources; i++) {
        totalMasses[i] = Float.NEGATIVE_INFINITY;
      }
    }

    @Override
    public void reduce(IntWritable nid, Iterable<PageRankNode> iterable, Context context) throws IOException, InterruptedException {
      Iterator<PageRankNode> values = iterable.iterator();

      PageRankNode node = new PageRankNode(); // Create the node structure that we're going to assemble back together from shuffled pieces.

      node.setType(PageRankNode.Type.Complete);
      node.setNodeId(nid.get());

      int massMessagesReceived = 0;
      int structureReceived = 0;

      float[] masses = new float[numOfSources];
      for (int i = 0; i < numOfSources; i++) {
        masses[i] = Float.NEGATIVE_INFINITY;
      }
      while (values.hasNext()) {
        PageRankNode n = values.next();

        if (n.getType().equals(PageRankNode.Type.Structure)) {
          // This is the structure; update accordingly.
          ArrayListOfIntsWritable list = n.getAdjacenyList();
          structureReceived++;
          node.setAdjacencyList(list);
        } else {
          // This is a message that contains PageRank mass; accumulate.
          float[] pageRanks = n.getPageRanks();
          for (int i = 0; i < numOfSources; i++) {
            masses[i] = sumLogProbs(masses[i], pageRanks[i]);
          }
          massMessagesReceived++;
        }
      }

      // Update the final accumulated PageRank mass.
      node.setPageRanks(masses);
      context.getCounter(PageRank.massMessagesReceived).increment(massMessagesReceived);

      // Error checking.
      if (structureReceived == 1) {
        // Everything checks out, emit final node structure with updated PageRank value.
        context.write(nid, node);

        // Keep track of total PageRank mass.
        for (int i = 0; i < numOfSources; i++) {
          totalMasses[i] = sumLogProbs(totalMasses[i], masses[i]);
        }
        //totalMass = sumLogProbs(totalMass, mass);
      } else if (structureReceived == 0) {
        // We get into this situation if there exists an edge pointing to a node which has no corresponding node structure (i.e., PageRank mass was passed to a non-existent node)...
        // log and count but move on.
        context.getCounter(PageRank.missingStructure).increment(1);
        LOG.warn("No structure received for nodeid: " + nid.get() + " mass: " + massMessagesReceived);
        // It's important to note that we don't add the PageRank mass to total... if PageRank mass was sent to a non-existent node, it should simply vanish.
      } else {
        // This shouldn't happen!
        throw new RuntimeException("Multiple structure received for nodeid: " + nid.get() + " mass: " + massMessagesReceived + " struct: " + structureReceived);
      }
    }

    @Override
    public void cleanup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();
      String taskId = conf.get("mapred.task.id");
      String path = conf.get("PageRankMassPath");

      Preconditions.checkNotNull(taskId);
      Preconditions.checkNotNull(path);

      // Write to a file the amount of PageRank mass we've seen in this reducer.
      FileSystem fs = FileSystem.get(context.getConfiguration());
      FSDataOutputStream out = fs.create(new Path(path + "/" + taskId), false);
      for (int i = 0; i < numOfSources; i++) {
        out.writeFloat(totalMasses[i]);
      }
      out.close();
    }
  }

  // Mapper that distributes the missing PageRank mass (lost at the dangling nodes) and takes care of the random jump factor.
  private static class MapPageRankMassDistributionClass extends Mapper<IntWritable, PageRankNode, IntWritable, PageRankNode> {
    private int[] sourceIds;
    private float[] missingMasses;
    private int nodeCnt = 0;

    @Override
    public void setup(Context context) throws IOException {
      Configuration conf = context.getConfiguration();

      int numOfSources = conf.getInt("NumOfSources", 0);
      missingMasses = new float[numOfSources];
      for (int i = 0; i < numOfSources; i++) {
        missingMasses[i] = conf.getFloat("MissingMass"+i, 0.0f);
      }
      String[] sources = conf.get("sources", null).split(",");
      sourceIds = new int[numOfSources];
      for (int i = 0; i < numOfSources; i++) {
        sourceIds[i] = Integer.parseInt(sources[i]);
      }

      nodeCnt = conf.getInt("NodeCount", 0);
    }

    @Override
    public void map(IntWritable nid, PageRankNode node, Context context) throws IOException, InterruptedException {
      float[] pageRanks = node.getPageRanks();
      float a = (float) (Math.log(ALPHA));
      float oneMinusA = (float) (Math.log(1.0f - ALPHA));
      for (int i = 0; i < pageRanks.length; i++) {
        pageRanks[i] += (float) Math.log(1.0f - ALPHA);
        if (node.getNodeId() == sourceIds[i]) {
          pageRanks[i] = sumLogProbs(pageRanks[i], a);
          float link = oneMinusA + (missingMasses[i] <= 0.0f ? Float.NEGATIVE_INFINITY : (float) Math.log(missingMasses[i]));
          pageRanks[i] = sumLogProbs(pageRanks[i], link);
        }
      }
      node.setPageRanks(pageRanks);

      context.write(nid, node);
    }
  }

  // Random jump factor.
  private static float ALPHA = 0.15f;
  private static NumberFormat formatter = new DecimalFormat("0000");

  /**
   * Dispatches command-line arguments to the tool via the {@code ToolRunner}.
   */
  public static void main(String[] args) throws Exception {
    ToolRunner.run(new RunPersonalizedPageRankBasic(), args);
  }

  public RunPersonalizedPageRankBasic() {}

  private static final String BASE = "base";
  private static final String NUM_NODES = "numNodes";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SOURCES = "sources";

  /**
   * Runs this tool.
   */
  @SuppressWarnings({ "static-access" })
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withArgName("path").hasArg().withDescription("base path").create(BASE));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("start iteration").create(START));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("end iteration").create(END));
    options.addOption(OptionBuilder.withArgName("num").hasArg().withDescription("number of nodes").create(NUM_NODES));
    options.addOption(OptionBuilder.withArgName("sources").hasArg().withDescription("source nodes").create(SOURCES));

    CommandLine cmdline;
    CommandLineParser parser = new GnuParser();

    try {
      cmdline = parser.parse(options, args);
    } catch (ParseException exp) {
      System.err.println("Error parsing command line: " + exp.getMessage());
      return -1;
    }

    if (!cmdline.hasOption(BASE) || !cmdline.hasOption(START) || !cmdline.hasOption(END) || !cmdline.hasOption(NUM_NODES) || !cmdline.hasOption(SOURCES)) {
      System.out.println("args: " + Arrays.toString(args));
      HelpFormatter formatter = new HelpFormatter();
      formatter.setWidth(120);
      formatter.printHelp(this.getClass().getName(), options);
      ToolRunner.printGenericCommandUsage(System.out);
      return -1;
    }

    String basePath = cmdline.getOptionValue(BASE);
    int n = Integer.parseInt(cmdline.getOptionValue(NUM_NODES));
    int s = Integer.parseInt(cmdline.getOptionValue(START));
    int e = Integer.parseInt(cmdline.getOptionValue(END));
    String sources = cmdline.getOptionValue(SOURCES);

    LOG.info("Tool name: RunPersonalizedPageRank");
    LOG.info(" - base path: " + basePath);
    LOG.info(" - num nodes: " + n);
    LOG.info(" - start iteration: " + s);
    LOG.info(" - end iteration: " + e);
    LOG.info(" - sources: " + sources);

    // Iterate PageRank.
    for (int i = s; i < e; i++) {
      iteratePageRank(i, i + 1, basePath, n, sources);
    }

    return 0;
  }

  // Run each iteration.
  private void iteratePageRank(int a, int b, String basePath, int numNodes, String sources) throws Exception {
    // Each iteration consists of two phases (two MapReduce jobs).

    // Job 1: distribute PageRank mass along outgoing edges.
    float[] masses = phase1(a, b, basePath, numNodes, sources);

    // Find out how much PageRank mass got lost at the dangling nodes.
    float[] missings = new float[masses.length]; 
    for (int i = 0; i < masses.length; i++) {
      missings[i] = 1.0f - (float) StrictMath.exp(masses[i]);
    }

    // Job 2: distribute missing mass, take care of random jump factor.
    phase2(a, b, missings, basePath, numNodes, sources);
  }

  private float[] phase1(int a, int b, String basePath, int numNodes, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + b + ":Phase1");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String in = basePath + "/iter" + formatter.format(a);
    String out = basePath + "/iter" + formatter.format(b) + "t";
    String outm = out + "-mass";

    // We need to actually count the number of part files to get the number of partitions (because the directory might contain _log).
    int numPartitions = 0;
    for (FileStatus s : FileSystem.get(getConf()).listStatus(new Path(in))) {
      if (s.getPath().getName().contains("part-"))
        numPartitions++;
    }

    LOG.info("PageRank: iteration " + b + ": Phase1");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);
    LOG.info(" - nodeCnt: " + numNodes);
    LOG.info("computed number of partitions: " + numPartitions);

    int numReduceTasks = numPartitions;

    job.getConfiguration().setInt("NodeCount", numNodes);
    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    //job.getConfiguration().set("mapred.child.java.opts", "-Xmx2048m");
    job.getConfiguration().set("PageRankMassPath", outm);
    int numOfSources = sources.split(",").length;
    job.getConfiguration().setInt("NumOfSources", numOfSources);

    job.setNumReduceTasks(numReduceTasks);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapClass.class);
    job.setCombinerClass(CombineClass.class);
    job.setReducerClass(ReduceClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);
    FileSystem.get(getConf()).delete(new Path(outm), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");

    float[] masses = new float[numOfSources];
    for (int i = 0; i < masses.length; i++) {
      masses[i] = Float.NEGATIVE_INFINITY;
    }
    FileSystem fs = FileSystem.get(getConf());
    for (FileStatus f : fs.listStatus(new Path(outm))) {
      FSDataInputStream fin = fs.open(f.getPath());
      for (int i = 0; i < masses.length; i++) {
        masses[i] = sumLogProbs(masses[i], fin.readFloat());
      }
      fin.close();
    }

    return masses;
  }

  private void phase2(int a, int b, float[] missings, String basePath, int numNodes, String sources) throws Exception {
    Job job = Job.getInstance(getConf());
    job.setJobName("PageRank:Basic:iteration" + b + ":Phase2");
    job.setJarByClass(RunPersonalizedPageRankBasic.class);

    String missingsStr = "";
    for (int i = 0; i < missings.length; i++) {
      missingsStr += missings[i]+" ";
    }
    LOG.info("missing PageRank masses: " + missingsStr);
    LOG.info("number of nodes: " + numNodes);

    String in = basePath + "/iter" + formatter.format(b) + "t";
    String out = basePath + "/iter" + formatter.format(b);

    LOG.info("PageRank: iteration " + b + ": Phase2");
    LOG.info(" - input: " + in);
    LOG.info(" - output: " + out);

    job.getConfiguration().setBoolean("mapred.map.tasks.speculative.execution", false);
    job.getConfiguration().setBoolean("mapred.reduce.tasks.speculative.execution", false);
    job.getConfiguration().set("sources", sources);
    job.getConfiguration().setInt("NumOfSources", missings.length);
    for (int i = 0; i < missings.length; i++) {
      job.getConfiguration().setFloat("MissingMass"+i, (float) missings[i]);
    }
    //job.getConfiguration().setFloat("MissingMass", (float) missing);
    job.getConfiguration().setInt("NodeCount", numNodes);

    job.setNumReduceTasks(0);

    FileInputFormat.setInputPaths(job, new Path(in));
    FileOutputFormat.setOutputPath(job, new Path(out));

    job.setInputFormatClass(NonSplitableSequenceFileInputFormat.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setMapOutputKeyClass(IntWritable.class);
    job.setMapOutputValueClass(PageRankNode.class);
    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(PageRankNode.class);

    job.setMapperClass(MapPageRankMassDistributionClass.class);

    FileSystem.get(getConf()).delete(new Path(out), true);

    long startTime = System.currentTimeMillis();
    job.waitForCompletion(true);
    System.out.println("Job Finished in " + (System.currentTimeMillis() - startTime) / 1000.0 + " seconds");
  }

  // Adds two log probs.
  private static float sumLogProbs(float a, float b) {
    if (a == Float.NEGATIVE_INFINITY)
      return b;

    if (b == Float.NEGATIVE_INFINITY)
      return a;

    if (a < b) {
      return (float) (b + StrictMath.log1p(StrictMath.exp(a - b)));
    }

    return (float) (a + StrictMath.log1p(StrictMath.exp(b - a)));
  }
}
