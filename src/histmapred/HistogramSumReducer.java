package histmapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.Iterator;

import static histmapred.Utils.makeHadoopArray;

/**
 * User: azat, Date: 14.05.13, Time: 1:18
 */
public class HistogramSumReducer extends MapReduceBase implements Reducer<Text, IntArrayWritable, Text, IntArrayWritable> {

    @Override
    public void reduce(Text key, Iterator<IntArrayWritable> values,
                       OutputCollector<Text, IntArrayWritable> output, Reporter reporter) throws IOException {
        int[] sum = new int[256];
        while (values.hasNext()) {
            Writable[] hist = values.next().get();
            for (int i = 0; i < sum.length; i++) {
                sum[i] += ((IntWritable) hist[i]).get();
            }
        }
        output.collect(key, makeHadoopArray(sum));
    }
}
