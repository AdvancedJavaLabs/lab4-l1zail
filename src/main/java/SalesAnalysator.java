import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import sort.Value;
import sort.ValueMapper;
import sort.ValueReducer;

public class SalesAnalysator {

    public static void main(String[] var0) throws Exception {
        Configuration conf = new Configuration();
        String inputDir = var0[1];
        String outputDir = var0[2];
        int numMappers = Integer.parseInt(var0[3]);
        int splitSize = Integer.parseInt(var0[4]);
        conf.setInt("mapreduce.job.maps", numMappers);
        conf.setInt("mapreduce.input.fileinputformat.split.maxsize", splitSize);

        String outputDirMiddle = outputDir +"middle";

        Job job = Job.getInstance(conf, "Category Analysis");
        job.setNumReduceTasks(numMappers);
        job.setJarByClass(SalesAnalysator.class);
        job.setMapperClass(SalesMapper.class);
        job.setReducerClass(SalesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Sales.class);
        FileInputFormat.addInputPath(job, new Path(inputDir));
        FileOutputFormat.setOutputPath(job, new Path(outputDirMiddle));


        boolean completion = job.waitForCompletion(true);

        if (!completion) {
            System.exit(1);
        }

        Job sortJob = Job.getInstance(conf, "Sorting");
        sortJob.setJarByClass(SalesAnalysator.class);
        sortJob.setMapperClass(ValueMapper.class);
        sortJob.setReducerClass(ValueReducer.class);
        sortJob.setOutputKeyClass(DoubleWritable.class);
        sortJob.setOutputValueClass(Value.class);
        FileInputFormat.addInputPath(sortJob, new Path(outputDirMiddle));
        FileOutputFormat.setOutputPath(sortJob, new Path(outputDir));

        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
    }
}
