import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import java.io.*;
import java.util.*;

import histmapred.VideoHistogramComputer;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapred.JobConf;
import org.junit.*;

/**
 * User: azat, Date: 14.05.13, Time: 15:48
 */
public class VideoHistogramComputerTest {

    @Test
    public void singleFileTest() throws Exception {
        JobConf conf = new JobConf();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        Path input = new Path("test_data/input");
        Path output = new Path("test_data/output");
        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output
        VideoHistogramComputer driver = new VideoHistogramComputer();
        driver.setConf(conf);

        int exitCode = runDriverWithParams(driver, new String[]{input.toString(), output.toString(), String.valueOf(4)});
        assertThat(exitCode, equalTo(0));

        List<String> lines = readOutputLines(output.toString() + "/part-00000");
        assertThat(lines.size(), equalTo(3)); // always three channels

        long predSum = -1;
        for (Iterator<String> i = lines.iterator(); i.hasNext();) {
            int[] hist = parseHist(i.next());
            long sum = 0;
            for (int j = 0; j < hist.length; j++) {
                sum += hist[j];
            }
            if (predSum != -1) {
                assertThat(sum, equalTo(predSum)); // picture size is the same for all channels
            }
            predSum = sum;
        }
    }

    private static int runDriverWithParams(VideoHistogramComputer driver, String[] params) throws IOException {
        return driver.run(params);
    }

    private static List<String> readOutputLines(String path) throws IOException {
        LinkedList<String> list = new LinkedList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));
        String line;
        while((line = reader.readLine()) != null) {
            list.add(line);
        }
        return list;
    }

    private static int[] parseHist(String line) {
        int lastTab = line.lastIndexOf("\t");
        String histSubstr = line.substring(lastTab + 2, line.length() - 1);
        String[] nums = histSubstr.split(",");
        int[] result = new int[nums.length];
        for (int i = 0; i < nums.length; i++) {
            result[i] = Integer.parseInt(nums[i]);
        }
        return result;
    }
}
