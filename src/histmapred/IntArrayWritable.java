package histmapred;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * User: azat, Date: 13.05.13, Time: 20:45
 */
public class IntArrayWritable extends ArrayWritable {

    public IntArrayWritable() {
        super(IntWritable.class);
    }

    @Override
    public String toString() {
        StringBuffer buffer = new StringBuffer("[");
        IntWritable[] array = (IntWritable[]) this.get();
        for (int i = 0; i < array.length; i++) {
            buffer.append(array[i].toString());
            if (i != array.length - 1) {
                buffer.append(",");
            }
        }
        buffer.append("]");
        return buffer.toString();
    }
}
