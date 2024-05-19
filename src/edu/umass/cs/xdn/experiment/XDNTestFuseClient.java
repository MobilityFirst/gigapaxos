package edu.umass.cs.xdn.experiment;

import java.io.*;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/**
 * XDNTestFuseClient is a test script to validate that our FUSE based approach works,
 * and accessible via Java.
 */
public class XDNTestFuseClient {

    private static final String FUSELOG_APPLY_BIN = "/users/fadhil/fuse/apply";
    private static final String FUSELOG_STATEDIFF_FILE = "/tmp/statediffv2";
    private static final String BACKUP_STATE_DIR = "/users/fadhil/service-data-bak/";

    public static void main(String[] args) {
        String fsSocketFile = "/tmp/fuselog.sock";
        SocketChannel socketChannel;
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(fsSocketFile));

        try {
            // establish connection to the filesystem
            socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean isConnEstablished = socketChannel.connect(address);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return;
            }

            // send get command (g) to the filesystem
            System.out.println(">> sending command ...");
            socketChannel.write(ByteBuffer.wrap("g".getBytes()));

            // wait for response indicating the size of the statediff
            ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
            sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
            sizeBuffer.clear();
            System.out.println(">> reading response ...");
            int numRead = socketChannel.read(sizeBuffer);
            System.out.println(">> got " + numRead + " " + new String(sizeBuffer.array(), StandardCharsets.UTF_8));
            if (numRead < 8) {
                System.err.println("failed to read size of the statediff");
                return;
            }
            long statediffSize = sizeBuffer.getLong(0);
            System.out.println("statediff size=" + statediffSize);

            // read all the statediff
            ByteBuffer statediffBuffer = ByteBuffer.allocate((int) statediffSize);
            numRead = 0;
            while (numRead < statediffSize) {
                numRead += socketChannel.read(statediffBuffer);
            }
            System.out.println("complete reading statediff ...");

            // convert the statediff to String
            String statediffString = new String(statediffBuffer.array(), StandardCharsets.ISO_8859_1);

            // store statediff into an external file
            FileOutputStream outputStream = new FileOutputStream(FUSELOG_STATEDIFF_FILE);
            outputStream.write(statediffString.getBytes(StandardCharsets.ISO_8859_1));
            outputStream.close();

            // apply the statediff: preparing the shell command
            String applyCommand = String.format("%s %s", FUSELOG_APPLY_BIN, BACKUP_STATE_DIR);
            ProcessBuilder pb = new ProcessBuilder(applyCommand.split("\\s+"));
            var e = pb.environment();
            e.put("FUSELOG_STATEDIFF_FILE", FUSELOG_STATEDIFF_FILE);

            // apply the statediff: start the process and capture the output
            Process process = pb.start();
            InputStream errInputStream = process.getErrorStream();
            BufferedReader errBufferedReader = new BufferedReader(new InputStreamReader(errInputStream));
            String line;
            while ((line = errBufferedReader.readLine()) != null) {
                System.out.println(line);
            }

            // apply the statediff: get the return code
            int errCode = process.waitFor();
            System.out.println("error code = " + errCode);


        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
