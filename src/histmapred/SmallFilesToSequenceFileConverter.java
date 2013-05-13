package histmapred;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * User: azat, Date: 13.05.13, Time: 1:32
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    static class SequenceFileReducer extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

        @Override
        public void reduce(Text key, Iterator<IntArrayWritable> values,
                           OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
            while (values.hasNext()) {
                output.collect(key, values.next());
            }
        }
    }

    static class SequenceFileMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntArrayWritable> {

        private JobConf conf;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
        }

        @Override
        public void map(Text key, BytesWritable value, OutputCollector<Text, IntArrayWritable> output,
                        Reporter reporter) throws IOException {

            IntArrayWritable outputValue = new IntArrayWritable();
            IntWritable[] outputArray = new IntWritable[4];
            outputArray[0] = new IntWritable(1);
            outputArray[1] = new IntWritable(2);
            outputArray[2] = new IntWritable(3);
            outputArray[3] = new IntWritable(4);
            outputValue.set(outputArray);
            output.collect(key, outputValue);
        }
    }

    @Override
    public int run(String[] args) throws IOException {
        JobConf conf = new JobConf(getConf(), getClass());
        if (conf == null) {
            return -1;
        }
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.setInputFormat(CombineWholeFileInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntArrayWritable.class);

        conf.setMapperClass(SequenceFileMapper.class);
        conf.setCombinerClass(SequenceFileReducer.class);

        // turn off reducer
        conf.setNumReduceTasks(0);
        //conf.setReducerClass(IdentityReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }
}

