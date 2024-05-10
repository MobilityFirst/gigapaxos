package edu.umass.cs.xdn.experiment;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Base64;
import java.util.Random;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class SerializationDeserializationTest {

    // TODO: compare general compression between these libraries:
    //  - https://github.com/airlift/aircompressor
    //  - https://mvnrepository.com/artifact/org.apache.commons/commons-compress

    public static void main(String[] args) {
        // generate random 12KB data
        byte[] data = new byte[12000];
        new Random().nextBytes(data);
        int numTrial = 5000;
        System.out.printf("Testing serialization-deserialization with %.2f KB data and %d trials\n",
                data.length/1000f, numTrial);

        // test compression speed
        byte[] finalCompressedData = null;
        long start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            byte[] compressedData = null;
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                DeflaterOutputStream dos = new DeflaterOutputStream(os);
                dos.write(data);
                dos.flush();
                dos.close();
                compressedData = os.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            finalCompressedData = compressedData;
        }
        long end = System.nanoTime();
        long elapsed = end - start;
        float avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg compression latency     : " + avgLatency + " µs/op");

        // test serialization speed
        String encodedData = null;
        start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            encodedData = Base64.getEncoder().encodeToString(finalCompressedData);
        }
        end = System.nanoTime();
        elapsed = end - start;
        avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg serialization latency   : " + avgLatency + " µs/op");

        // test deserialization speed
        byte[] decodedData = null;
        start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            decodedData = Base64.getDecoder().decode(encodedData);
        }
        end = System.nanoTime();
        elapsed = end - start;
        avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg deserialization latency : " + avgLatency + " µs/op");

        // test decompression speed
        byte[] decompressedData = null;
        start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            try {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                OutputStream ios = new InflaterOutputStream(os);
                ios.write(decodedData);
                ios.flush();
                ios.close();
                decompressedData = os.toByteArray();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        end = System.nanoTime();
        elapsed = end - start;
        avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg decompression latency   : " + avgLatency + " µs/op");

        assert Arrays.equals(data, decompressedData) : "decompressed data is not the same";
    }

}
