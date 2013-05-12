package histmapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.IdentityReducer;
import org.apache.hadoop.util.*;

/**
 * User: azat, Date: 13.05.13, Time: 1:32
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    static class SequenceFileMapper extends MapReduceBase
            implements Mapper<Text, BytesWritable, Text, BytesWritable> {

        private JobConf conf;

        @Override
        public void configure(JobConf conf) {
            this.conf = conf;
        }

        @Override
        public void map(Text key, BytesWritable value, OutputCollector<Text, BytesWritable> output,
                        Reporter reporter) throws IOException {
            output.collect(key, value);
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
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(BytesWritable.class);
        conf.setMapperClass(SequenceFileMapper.class);
        conf.setReducerClass(IdentityReducer.class);
        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }
}

