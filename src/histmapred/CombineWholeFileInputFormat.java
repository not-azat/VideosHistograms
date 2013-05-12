package histmapred;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileInputFormat;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * User: azat, Date: 13.05.13, Time: 1:24
 */
public class CombineWholeFileInputFormat extends CombineFileInputFormat<Text, BytesWritable> {

    @Override
    protected boolean isSplitable(FileSystem fs, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Text, BytesWritable> getRecordReader(
            InputSplit split, JobConf job, Reporter reporter) throws IOException {
        return new CombineWholeFileRecordReader((CombineFileSplit) split, job);
    }
}
