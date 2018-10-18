import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class main {
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: Ejercicio1 <input path> <output path>");
            System.exit(-1);
        }
        Configuration conf = new Configuration();
        conf.set("mapred.textoutputformat.separatorText", ",");
        Job job = new Job();
        job.setJarByClass(main.class);
        job.setJobName("Ejercicio1");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        conf.set("mapred.textoutputformat.separatorText", ",");
        job.setMapperClass(mapper.class);
        job.setReducerClass(reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.out.println("Done!");
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
