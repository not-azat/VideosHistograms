package histmapred;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

/**
 * User: azat, Date: 13.05.13, Time: 1:32
 */
public class VideoHistogramComputer extends Configured implements Tool {

    @Override
    public int run(String[] args) throws IOException {
        if (args.length != 3) {
            System.out.println("arguments: <input path> <output path> <number of frames>");
            return -1;
        }
        JobConf conf = new JobConf(getConf(), getClass());
        if (conf == null) {
            return -1;
        }
        FileInputFormat.addInputPath(conf, new Path(args[0]));
        FileOutputFormat.setOutputPath(conf, new Path(args[1]));
        conf.set("frames", args[2]);

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
        int exitCode = ToolRunner.run(new VideoHistogramComputer(), args);
        System.exit(exitCode);
    }
}

