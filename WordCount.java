import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;

public class WordCount {

    // Mapper<> 前兩參數 <Object, Text> 為 Map Input <key, value> 的資料型態
    // 後兩參數 <Text, IntWritable> 為 Map Output <key, value> 的資料型態
    public static class MyMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private String regex = "[^a-zA-Z ]";

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // Remove all the punctuation
            String line = value.toString().toLowerCase();
            line = line.replaceAll(regex, " ");

            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static class IntWritableDecreasingComparator extends IntWritable.Comparator {
        @Override
        public int compare(WritableComparable a, WritableComparable b){
            return -super.compare(a, b);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2){
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class InverseReducer extends Reducer<IntWritable, Text, Text, IntWritable> {
        public void reduce(IntWritable value, Iterable<Text> keys, Context context)
                throws IOException, InterruptedException {
            for (Text key : keys){
                context.write(key, value);
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        /* Compress Map side intermediate output */
        conf.set("mapreduce.map.output.compress", "true");
        conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.GzipCodec");
        
        /* Memory Allocation tunning */
        // conf.set("mapreduce.map.memory.mb", "2048");
        // conf.set("mapreduce.reduce.memory.mb", "2048");
        // conf.set("mapreduce.child.java.opts", "-Xmx2048m");
        // conf.set("mapreduce.task.io.sort.mb", "500");
        // conf.set("mapreduce.map.sort.spill.percent", "0.90");
        // conf.set("mapreduce.reduce.input.buffer.percent", "0.7");

        /* Number of simultaneous map task per job */
        // conf.set("mapreduce.job.running.map.limit", "2");

        /* If the output directory already exist, delete it first. */
        Path outputDir = new Path(args[1]);
        FileSystem fileSystem = outputDir.getFileSystem(conf);
        if (fileSystem.exists(outputDir)) {
            fileSystem.delete(outputDir, true); // true 代表「就算 output dir 內還有東西，也一併刪除」
        }


        
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        
        job.setMapperClass(MyMapper.class);
        job.setCombinerClass(MyReducer.class);
        job.setReducerClass(MyReducer.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        /* Sequence File Input/Output Format : 
         * Specific compressed binary file format which is optimized for passing data between
         * the output of one MapReduce job to the input of some other MapReduce job
         */
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        /* Combine File Input Format : 
         * Combine several small files into a inputsplit
         */
        // job.setInputFormatClass(CombineTextInputFormat.class);
        // conf.set("mapreduce.input.fileinputformat.split.maxsize", "134217728");  // 128 MB


        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        if(!job.waitForCompletion(true)){
            System.exit(1);
        }

        // ---------------------------------------------------------------------------------

        // 2nd job

        Job sortJob = Job.getInstance(conf, "sorted word count");
        sortJob.setJarByClass(WordCount.class);
        
        sortJob.setMapperClass(InverseMapper.class);
        sortJob.setSortComparatorClass(IntWritableDecreasingComparator.class);
        sortJob.setReducerClass(InverseReducer.class);

        sortJob.setOutputKeyClass(IntWritable.class);
        sortJob.setOutputValueClass(Text.class);

        sortJob.setInputFormatClass(SequenceFileInputFormat.class);   // 對應 1st job 的 output format
        sortJob.setOutputFormatClass(TextOutputFormat.class);         // TextOutput 是 default format

        FileInputFormat.addInputPath(sortJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(sortJob, new Path(args[1] + "/sorted"));

        System.exit(sortJob.waitForCompletion(true) ? 0 : 1);
    }
}
