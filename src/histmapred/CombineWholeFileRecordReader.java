package histmapred;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.CombineFileSplit;

import java.io.IOException;

/**
 * User: azat, Date: 13.05.13, Time: 1:28
 */
class CombineWholeFileRecordReader implements RecordReader<Text, BytesWritable> {

    private CombineFileSplit fileSplit;
    private Configuration conf;
    private int processed;
    private long processedBytes;
    private long numPaths;

    public CombineWholeFileRecordReader(CombineFileSplit fileSplit, Configuration conf) throws IOException {
        this.fileSplit = fileSplit;
        this.numPaths = fileSplit.getNumPaths();
        this.processed = 0;
        this.processedBytes = 0;
        this.conf = conf;
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public BytesWritable createValue() {
        return new BytesWritable();
    }

    @Override
    public long getPos() throws IOException {
        return processedBytes;
    }

    @Override
    public float getProgress() throws IOException {
        return processed / (float) this.numPaths;
    }

    @Override
    public boolean next(Text key, BytesWritable value) throws IOException {
        if (processed < numPaths) {
            byte[] contents = new byte[(int) fileSplit.getLengths()[processed]];
            Path file = fileSplit.getPath(processed);
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = null;
            try {
                in = fs.open(file);
                IOUtils.readFully(in, contents, 0, contents.length);
                value.set(contents, 0, contents.length);
                key.set(file.toString());
            } finally {
                IOUtils.closeStream(in);
            }
            processedBytes += fileSplit.getLengths()[processed];
            processed++;
            return true;
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        // do nothing
    }
}
