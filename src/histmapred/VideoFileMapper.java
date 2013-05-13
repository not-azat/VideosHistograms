package histmapred;

import com.xuggle.xuggler.*;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.awt.image.BufferedImage;
import java.awt.image.Raster;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static histmapred.Utils.makeHadoopArray;

/**
 * User: azat, Date: 13.05.13, Time: 23:39
 */
public class VideoFileMapper extends MapReduceBase implements Mapper<Text, BytesWritable, Text, IntArrayWritable> {

    private int count;

    @Override
    public void configure(JobConf job) {
        count = Integer.parseInt(job.get("frames"));
    }

    @Override
    public void map(Text key, BytesWritable value, OutputCollector<Text, IntArrayWritable> output,
                    Reporter reporter) throws IOException {

        BufferedImage[] images = processVideo(value.getBytes(), 0, count);
        for (int num = 0; num < images.length; num++) {
            writeImageKV(images[num], key.toString(), output);
        }
    }

    private static int[] computeThreeDepth(double[] samples) {
        int[] result = new int[3];
        result[0] = (int) samples[0];
        result[1] = (int) samples[1];
        result[2] = (int) samples[2];
        return result;
    }

    private static void writeImageKV(BufferedImage image, String fileName, OutputCollector<Text, IntArrayWritable> output)
            throws IOException {

        int[] dim0 = new int[256];
        int[] dim1 = new int[256];
        int[] dim2 = new int[256];
        Raster raster = image.getRaster();
        for (int x = 0; x < raster.getWidth(); x++) {
            for (int y = 0; y < raster.getHeight(); y++) {
                int[] depths = computeThreeDepth(raster.getPixel(x, y, new double[3]));
                dim0[depths[0]]++;
                dim1[depths[1]]++;
                dim2[depths[2]]++;
            }
        }
        output.collect(new Text(fileName + " dim1"), makeHadoopArray(dim0));
        output.collect(new Text(fileName + " dim2"), makeHadoopArray(dim1));
        output.collect(new Text(fileName + " dim3"), makeHadoopArray(dim2));
    }

    private static BufferedImage[] processVideo(byte[] bytes, long startTime, int count) throws IOException {
        IContainer container = IContainer.make();
        InputStream in = new ByteArrayInputStream(bytes);
        if (container.open(in, null) < 0)
            throw new IllegalArgumentException("could not open file");
        int numStreams = container.getNumStreams();
        int videoStreamId = -1;
        IStreamCoder videoCoder = null;
        for (int i = 0; i < numStreams; i++) {
            IStream stream = container.getStream(i);
            IStreamCoder coder = stream.getStreamCoder();
            if (coder.getCodecType() == ICodec.Type.CODEC_TYPE_VIDEO) {
                videoStreamId = i;
                videoCoder = coder;
                break;
            }
        }
        if (videoStreamId == -1)
            throw new RuntimeException("could not find video stream in container");
        if (videoCoder.open() < 0)
            throw new RuntimeException("could not open video decoder for container");

        IPacket packet = IPacket.make();
        long start = startTime * 1000 * 1000; // from the beginning
        long end = 200 * 1000 * 1000; // no more than 200 sec
        long step = 100 * 1000;
        int counter = 0;
        BufferedImage[] resultArray = new BufferedImage[count];

        END: while (container.readNextPacket(packet) >= 0 && counter < count) {
            if (packet.getStreamIndex() == videoStreamId) {
                IVideoPicture picture = IVideoPicture.make(
                        videoCoder.getPixelType(), videoCoder.getWidth(),
                        videoCoder.getHeight());
                int offset = 0;
                while (offset < packet.getSize()) {
                    int bytesDecoded = videoCoder.decodeVideo(picture, packet,
                            offset);
                    if (bytesDecoded < 0)
                        throw new RuntimeException("got error decoding video");
                    offset += bytesDecoded;
                    if (picture.isComplete()) {
                        IVideoPicture newPic = picture;
                        long timestamp = picture.getTimeStamp();
                        if (timestamp > start) {
                            BufferedImage javaImage = com.xuggle.xuggler.Utils.videoPictureToImage(newPic);
                            resultArray[counter++] = javaImage;
                            start += step;
                        }
                        if (timestamp > end) {
                            break END;
                        }
                    }
                }
            }
        }
        if (videoCoder != null) {
            videoCoder.close();
        }
        if (container != null) {
            container.close();
        }
        return resultArray;
    }
}
