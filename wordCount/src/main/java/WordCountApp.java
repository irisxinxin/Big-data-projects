import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class WordCountApp {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        //configuration.set("ssss", "ssss");

        Job job = Job.getInstance(configuration, "WordCount");

        // set job's parameters and classes
        job.setJarByClass(WordCountApp.class);
        job.setReducerClass(WordCount.WordCountReducer.class);
        job.setMapperClass(WordCount.WordCountMapper.class);

        // set reducer output key & value type
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // set mapper output key & value type
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);


//        // if there already exists output path, delete it
//        Path outputPath = new Path("output");
//        FileSystem fileSystem = FileSystem.get(configuration);
//


        // set input and output path
        FileOutputFormat.setOutputPath(job, new Path("output"));
        FileInputFormat.setInputPaths(job, new Path("input"));

        //submit job
        boolean res = job.waitForCompletion(true);

        System.exit(res ? 0 : -1);
    }

}
