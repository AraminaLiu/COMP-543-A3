import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.conf.Configured;
import java.util.ArrayList;
import java.util.List;
//import java.util.regex.Pattern;
//import java.util.regex.Matcher;

public class pair extends Configured implements Tool {

    static int printUsage() {
        System.out.println("find pairs [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }

    public static class pairMapper
            extends Mapper<Object, Text, Text, DoubleWritable> {

        // so we don't have to do reallocations
        //private final static IntWritable one = new IntWritable(1);
        //private Text word = new Text();
        //private Text prev = new Text();
        //private Text bigrams = new Text();
        //String prevToken = "1";
        // to chec for only alphanumeric
        //String expression = "^[a-zA-Z]*$";
        //Pattern pattern = Pattern.compile(expression);

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",");
            if(!parts[0].equals("medallion")){
                DoubleWritable rev = new DoubleWritable(Double.parseDouble(parts[10]));
                String driver_id = parts[1];
                Text taxi = new Text(driver_id);
                context.write(taxi, rev);

            }

        }
    }

    public static class pairReducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue calculate");
        job.setJarByClass(pair.class);
        job.setMapperClass(pairMapper.class);
        job.setCombinerClass(pairReducer.class);
        job.setReducerClass(pairReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        List<String> other_args = new ArrayList<String>();
        for(int i=0; i < args.length; ++i) {
            try {
                if ("-r".equals(args[i])) {
                    job.setNumReduceTasks(Integer.parseInt(args[++i]));
                } else {
                    other_args.add(args[i]);
                }
            } catch (NumberFormatException except) {
                System.out.println("ERROR: Integer expected instead of " + args[i]);
                return printUsage();
            } catch (ArrayIndexOutOfBoundsException except) {
                System.out.println("ERROR: Required parameter missing from " +
                        args[i-1]);
                return printUsage();
            }
        }
        // Make sure there are exactly 2 parameters left.
        if (other_args.size() != 2) {
            System.out.println("ERROR: Wrong number of parameters: " +
                    other_args.size() + " instead of 2.");
            return printUsage();
        }
        FileInputFormat.setInputPaths(job, other_args.get(0));
        FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
        return (job.waitForCompletion(true) ? 0 : 1);
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new pair(), args);
        System.exit(res);
    }

}
