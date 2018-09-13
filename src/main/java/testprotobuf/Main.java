package testprotobuf;

import com.google.protobuf.ByteString;
import org.iq80.snappy.Snappy;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class Main {

    public static final int SIZE = 10000;

    public static void main(String[] args) {

        byte[] bytes = new byte[SIZE];
        int j = 13;
        for (int i = 0; i < SIZE; i++) {
            bytes[i] = (byte)(i * j++);
        }

        TestProtos.MyMessage.Builder builder = TestProtos.MyMessage.newBuilder();
        ByteString bs = ByteString.copyFrom(bytes);
        builder.setByteArray(bs);
        TestProtos.MyMessage message = builder.build();

        int maxCompressedSize = Snappy.maxCompressedLength(bytes.length);
        byte[] compressed = new byte[maxCompressedSize]; // size for worst case
        int compressedSize = Snappy.compress(bytes, 0, bytes.length, compressed, 0);

        ImmutableBytesWritable bytesWritable = new ImmutableBytesWritable();
        bytesWritable.set(compressed, 0, compressedSize);  // Corresponding call in phoenix: ptr.set(compressed,0,compressedSize);

        TestProtos.MyMessage.Builder compressedBuilder = TestProtos.MyMessage.newBuilder();
        ByteString compressedByteString = ByteString.copyFrom(bytesWritable.get());
        compressedBuilder.setByteArray(compressedByteString);
        TestProtos.MyMessage compressedMessage = compressedBuilder.build();

        System.out.println("bytes size: " + bytes.length + " bytes");
        System.out.println("compressed bytes size: " + compressedSize + " bytes");
        System.out.println("message size: " + message.getSerializedSize() + " bytes");
        System.out.println("compressed message size: " + compressedMessage.getSerializedSize() + " bytes");
    }
}
