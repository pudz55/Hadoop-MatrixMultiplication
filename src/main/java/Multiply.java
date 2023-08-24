import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.ReflectionUtils;



class Elem implements Writable {
    int t;
    int ndx;
    double value;

    Elem() {
        t = 0;
        ndx = 0;
        value = 0.0;
    }

    Elem(int t, int ndx, double value) {
        this.t = t;
        this.ndx = ndx;
        this.value = value;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        t = input.readInt();
        ndx = input.readInt();
        value = input.readDouble();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(t);
        output.writeInt(ndx);
        output.writeDouble(value);
    }
}

class Pair implements WritableComparable<Pair> {

    int i;
    int j;

    Pair() {
        i = 0;
        j = 0;
    }

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        i = input.readInt();
        j = input.readInt();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeInt(i);
        output.writeInt(j);
    }

    @Override
    public int compareTo(Pair o) {

        if(i==o.i)
            return j-o.j;
        else
            return i-o.i;
    }

    public String toString() {
        return i + " " + j + " ";
    }
}

public class Multiply {

    public static class Mapper_M extends Mapper <Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          
            String lines = value.toString();
            String[] str = lines.split(",");
            int i = Integer.parseInt(str[0]);
            int j = Integer.parseInt(str[1]);
            double v = Double.parseDouble(str[2]);
            
            Elem elem = new Elem(0, i, v);
            
            IntWritable keyValue = new IntWritable(j);
            
            context.write(keyValue, elem);
        }
    }

     public static class Mapper_N extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          
            String lines = value.toString();
            String[] str = lines.split(",");
            int i = Integer.parseInt(str[0]);
            int j = Integer.parseInt(str[1]);
            double v = Double.parseDouble(str[2]);
            
            Elem elem = new Elem(1, j, v);
            
            IntWritable keyValue = new IntWritable(i);
            
            context.write(keyValue, elem);
        }
    }

    public static class Reducer_1 extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context ) throws IOException, InterruptedException {
          
            Vector<Elem> M = new Vector<Elem>();
            Vector<Elem> N = new Vector<Elem>();
            Configuration conf = context.getConfiguration();
            
            for(Elem Elem : values) {
                Elem tempElem = ReflectionUtils.newInstance(Elem.class, conf);
                ReflectionUtils.copy(conf, Elem, tempElem);
                if (tempElem.t == 0) {
                    M.add(tempElem);
                } else if(tempElem.t == 1) {
                    N.add(tempElem);
                }
            }

            for ( Elem a: M){
                for ( Elem b: N)
                    {
                        double val = (a.value * b.value);
                        
                        context.write(new Pair(a.ndx,b.ndx),new DoubleWritable(val));
                    }
            }
        }
    }

    public static class Mapper_MN extends Mapper <Object,Text,Pair,DoubleWritable> {
        @Override
        public void map ( Object key, Text value, Context context ) throws IOException, InterruptedException {
          
            String lines = value.toString();
            String[] str = lines.split(" ");
            int i=Integer.parseInt(str[0]);
            int j=Integer.parseInt(str[1]);
            double values=Double.parseDouble(str[2]);
            Pair p = new Pair(i,j);
            DoubleWritable double_value = new DoubleWritable(values);
            
            context.write(p, double_value);
        }
    }

    public static class Reducer_2 extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce(Pair key, Iterable<DoubleWritable> values, Context context)throws IOException, InterruptedException{
          
            double res = 0.0;
            
            for(DoubleWritable val : values) {
                res += val.get();
            }
            context.write(key, new DoubleWritable(res));
            
        }
    }

    public static void main ( String[] args ) throws Exception {

        Job job1 = Job.getInstance();
        
        job1.setJobName("Map_Reduce_Job1");
        job1.setJarByClass(Multiply.class);
        
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Mapper_M.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Mapper_N.class);
        
        job1.setReducerClass(Reducer_1.class);
        FileOutputFormat.setOutputPath(job1, new Path(args[2]));
        
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        
        job1.setOutputFormatClass(TextOutputFormat.class);
        
        job1.waitForCompletion(true);


        Job job2 = Job.getInstance();
        
        job2.setJobName("Map_Reduce_Job2");
        job2.setJarByClass(Multiply.class);
        job2.setMapperClass(Mapper_MN.class);
        job2.setReducerClass(Reducer_2.class);
        FileInputFormat.setInputPaths(job2, new Path(args[2]));
        
        FileOutputFormat.setOutputPath(job2, new Path(args[3]));
        
        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        
        job2.waitForCompletion(true);
    }
}
