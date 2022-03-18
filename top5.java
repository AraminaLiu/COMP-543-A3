import java.io.IOException;
//import java.util.StringTokenizer;
import java.io.*;
import java.util.*;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
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

class pair implements Comparable<pair>{
    private String taxi;
    private Double rev;

    public int compareTo(pair p1) {
        if (this.rev > p1.getrev()) {
            return 1;
        } else {
            return -1;
        }
    }

    public pair(String taxi, Double rev) {
        this.taxi = taxi;
        this.rev = rev;
    }


    public String toString() {
        return taxi + ", " + rev;
    }

    public String getTaxi(){
        return this.taxi;
    }

    public Double getrev(){
        return this.rev;
    }


}


public class top5 extends Configured implements Tool {

    static int printUsage() {
        System.out.println("revenue calculate [-m <maps>] [-r <reduces>] <input> <output>");
        ToolRunner.printGenericCommandUsage(System.out);
        return -1;
    }




    public static class top5Mapper
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

        private PriorityQueue<pair> queue = new PriorityQueue<pair>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            Double R = Double.parseDouble(parts[1]);
            String T = parts[0];

            if(!R.isNaN() & !T.isEmpty()){
                queue.offer(new pair(T, R));
            }

            if (queue.size() > 5)
            {
                queue.poll();
            }


        }

        public void cleanup(Context context) throws IOException,
                InterruptedException
        {
            while(!queue.isEmpty()){
                pair obj = queue.poll();
                String taxi = obj.getTaxi();
                Double rev = obj.getrev();
                context.write(new Text(taxi), new DoubleWritable(rev));
                // obj is the next ordered item in the queue
            }
        }
    }

    public static class top5Reducer
            extends Reducer<Text, DoubleWritable,Text,DoubleWritable> {

        private PriorityQueue<pair> queue2 = new PriorityQueue<pair>();
        private DoubleWritable sum_d = new DoubleWritable();
        public void reduce(Text key, Iterable<DoubleWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            String taxi = key.toString();
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum = val.get();
            }
            queue2.add(new pair(taxi, sum));

            if (queue2.size() > 5)
            {
                queue2.poll();
            }



        }

        public void cleanup(Context context) throws IOException,
                InterruptedException
        {
            while(!queue2.isEmpty()){
                pair obj = queue2.poll();
                String taxi = obj.getTaxi();
                Double rev = obj.getrev();
                context.write(new Text(taxi), new DoubleWritable(rev));
                // obj is the next ordered item in the queue
            }
        }
    }

    public int run(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "revenue calculate");
        job.setJarByClass(top5.class);
        job.setMapperClass(top5Mapper.class);
        job.setCombinerClass(top5Reducer.class);
        job.setReducerClass(top5Reducer.class);
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
        int res = ToolRunner.run(new Configuration(), new top5(), args);

        System.exit(res);
    }

}

