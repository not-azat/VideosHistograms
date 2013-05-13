package histmapred;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

/**
 * User: azat, Date: 13.05.13, Time: 1:32
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

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

        conf.setMapperClass(VideoFileMapper.class);
        conf.setCombinerClass(HistogramSumReducer.class);
        conf.setReducerClass(HistogramSumReducer.class);

        JobClient.runJob(conf);
        return 0;
    }

    public static void main(String[] args) throws Exception {
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
        System.exit(exitCode);
    }
}

