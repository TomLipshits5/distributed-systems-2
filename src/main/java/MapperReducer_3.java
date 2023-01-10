import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class MapperReducer_3 {
    private static boolean IsNumeric(String s){
        for(Character c : s.toCharArray()){
            if(!Character.isDigit(c))
                return false;
        }
        return true;
    }
    public static class Mapper_3 extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            List<String> valueList = Arrays.asList(value.toString().split(" "));
            if (IsNumeric(valueList.get(0))){
                IntWritable r = new IntWritable(Integer.parseInt(valueList.get(1)));
                context.write(r, new Text(valueList.get(1) + " " + valueList.get(2)));
            }else{
                Text trigram = new Text(valueList.get(0));
                IntWritable r = new IntWritable(Integer.parseInt(valueList.get(1)));
                context.write(r, trigram);
            }
        }

    }

    public static class Comperator_3 extends Text.Comparator{
        public int compare(Text t1, Text t2){
            String[] l1 = t1.toString().split(" ");
            String[] l2 = t2.toString().split(" ");
            if (IsNumeric(l1[1]))
                return 1;
            else if(IsNumeric(l2[1]))
                return -1;
            else
                return 0;
        }
    }

    public static class Reducer_3 extends Reducer<IntWritable, Text, Text, FloatWritable>{
        @Override
        public void reduce(IntWritable r, Iterable<Text> trigrams, Context context) throws IOException, InterruptedException {
            FloatWritable res = new FloatWritable();
            Iterator<Text> it = trigrams.iterator();
            if (it.hasNext()){
                String[] numValues = it.next().toString().split(" ");
                float T = Float.parseFloat(numValues[0]);
                float N = Float.parseFloat(numValues[1]);
                res.set(T / N);
            }

            while(it.hasNext()){
                Text trigram = it.next();
                context.write(trigram, res);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Step_3");
        job.setJarByClass(MapperReducer_3.class);
        job.setMapperClass(MapperReducer_3.Mapper_3.class);
        job.setSortComparatorClass(Comperator_3.class);
        job.setPartitionerClass(Partitioner.class);
        job.setReducerClass(MapperReducer_3.Reducer_3.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
