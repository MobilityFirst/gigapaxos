package edu.umass.cs.xdn.experiment;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Random;

public class EncodingTest {

    public static void main(String[] args) {
        // generate random 12KB data
        byte[] data = new byte[12000];
        new Random().nextBytes(data);
        int numTrial = 5000;
        System.out.printf("Testing encoding with %.2f KB data and %d trials\n",
                data.length / 1000f, numTrial);

        // test Base64 encoding
        String encodedStringBase64 = null;
        long start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            encodedStringBase64 = Base64.getEncoder().encodeToString(data);
        }
        long end = System.nanoTime();
        long elapsed = end - start;
        float avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg base64 encoding latency    : " + avgLatency + " µs/op");
        System.out.println("   encoded substring: '" + encodedStringBase64.substring(0, 10) + "'");
        System.out.println("   length           : " + encodedStringBase64.length());

        // test ISO encoding
        String encodedStringISO8859 = null;
        start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            encodedStringISO8859 = new String(data, StandardCharsets.ISO_8859_1);
        }
        end = System.nanoTime();
        elapsed = end - start;
        float avgLatencyISO = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg ISO8859 encoding latency   : " + avgLatencyISO + " µs/op");
        System.out.println("   encoded substring: '" + encodedStringISO8859.substring(0, 10) + "'");
        System.out.println("   length           : " + encodedStringISO8859.length());

        System.out.printf("\n>> Conclusion: ISO is %.2fx faster than Base64 for encoding\n",
                avgLatency / avgLatencyISO);

        decodingTest();
    }

    private static void decodingTest() {
        // generate random 12KB data
        byte[] dataBytes = new byte[12000];
        new Random().nextBytes(dataBytes);
        String data = Base64.getEncoder().encodeToString(dataBytes);
        int numTrial = 5000;
        System.out.printf("\n\nTesting decoding with %.2f KB String data and %d trials\n",
                data.length() / 1000f, numTrial);

        // test Base64 decoding
        byte[] decodedBase64 = null;
        long start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            decodedBase64 = Base64.getDecoder().decode(data);
        }
        long end = System.nanoTime();
        long elapsed = end - start;
        float avgLatency = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg base64 decoding latency    : " + avgLatency + " µs/op");
        System.out.println("   decoded byte[0]: " + decodedBase64[0]);

        // test ISO8859 decoding
        byte[] decodedISO8859 = null;
        start = System.nanoTime();
        for (int i = 0; i < numTrial; i++) {
            decodedISO8859 = data.getBytes(StandardCharsets.ISO_8859_1);
        }
        end = System.nanoTime();
        elapsed = end - start;
        float avgLatencyISO = elapsed / (float) numTrial / 1000f;
        System.out.println(">> avg ISO8859 decoding latency   : " + avgLatency + " µs/op");
        System.out.println("   decoded byte[0]: " + decodedISO8859[0]);

        System.out.printf("\n>> Conclusion: ISO is %.2fx faster than Base64 for decoding\n",
                avgLatency / avgLatencyISO);

    }

}
