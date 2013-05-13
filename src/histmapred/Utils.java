package histmapred;

import org.apache.hadoop.io.IntWritable;

/**
 * User: azat, Date: 14.05.13, Time: 1:26
 */
public class Utils {

    public static IntArrayWritable makeHadoopArray(int[] source) {
        IntArrayWritable result = new IntArrayWritable();
        IntWritable[] array = new IntWritable[source.length];
        for (int i = 0; i < source.length; i++) {
            array[i] = new IntWritable(source[i]);
        }
        result.set(array);
        return result;
    }
}
