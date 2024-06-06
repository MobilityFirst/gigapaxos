package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.utils.Shell;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String FUSELOG_BIN_PATH = "/usr/bin/fuselog";
    private static final String FUSELOG_APPLY_BIN_PATH = "/usr/bin/fuselog-apply";

    private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuselog/";

    private final String baseMountDirPath;
    private final String baseSocketDirPath;
    private final String baseDiffDirPath;

    // important locations:
    // - /tmp/xdn/state/fuselog/<node-id>/                          the base directory
    // - /tmp/xdn/state/fuselog/<node-id>/mnt/                      the mount directory
    // - /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/       mount dir of specific service
    // - /tmp/xdn/state/fuselog/<node-id>/sock/                     the socket directory
    // - /tmp/xdn/state/fuselog/<node-id>/sock/<service-name>.sock  socket to fs of specific service
    // - /tmp/xdn/state/fuselog/<node-id>/diff/                     the stateDiff directory

    private Map<String, SocketChannel> serviceFSSocket;

    public FuselogStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/");

        // make sure that fuselog and fuselog-apply are exist
        File fuselog = new File(FUSELOG_BIN_PATH);
        assert fuselog.exists() : "fuselog binary does not exist at " + FUSELOG_BIN_PATH;
        File fuselogApplicator = new File(FUSELOG_APPLY_BIN_PATH);
        assert fuselogApplicator.exists() : "fuselog-apply binary does not exist at " +
                FUSELOG_APPLY_BIN_PATH;

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath + "mnt/";
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create socket dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/sock/
        this.baseSocketDirPath = defaultWorkingBasePath + nodeID + "/sock/";
        try {
            Files.createDirectories(Paths.get(this.baseSocketDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create diff dir, if not exist
        // e.g., /tmp/xdn/state/fuselog/node1/diff/
        this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
        try {
            Files.createDirectories(Paths.get(this.baseDiffDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // initialize mapping between serviceName to the FS socket
        this.serviceFSSocket = new ConcurrentHashMap<>();

    }

    @Override
    public String getTargetDirectory(String serviceName) {
        return baseMountDirPath + serviceName + "/";
    }

    @Override
    public boolean preInitialization(String serviceName) {
        String targetDir = this.getTargetDirectory(serviceName);
        String socketFile = baseSocketDirPath + serviceName + ".sock";

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/
        try {
            Shell.runCommand("rm -rf " + targetDir);
            Files.createDirectory(Paths.get(targetDir));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // initialize file system in the mnt dir, with socket
        assert targetDir.length() > 1 : "invalid target mount directory";
        // remove the trailing '/' at the end of targetDir
        String targetDirPath = targetDir.substring(0, targetDir.length() - 1);
        String cmd = String.format("%s -s -o allow_other -o allow_root %s",
                FUSELOG_BIN_PATH, targetDirPath);
        Map<String, String> env = new HashMap<>();
        env.put("FUSELOG_SOCKET_FILE", socketFile);
        int exitCode = Shell.runCommand(cmd, false, env);
        assert exitCode == 0 : "failed to mount filesystem with exit code " + exitCode;

        // initialize socket client for the filesystem
        SocketChannel socketChannel;
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(socketFile));
        try {
            socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
            boolean isConnEstablished = socketChannel.connect(address);
            if (!isConnEstablished) {
                System.err.println("failed to connect to the filesystem");
                return false;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        serviceFSSocket.put(serviceName, socketChannel);

        return true;
    }

    @Override
    public boolean postInitialization(String serviceName) {
        // TODO: read initialization stateDiff and discard it
        return false;
    }

    @Override
    public String captureStateDiff(String serviceName) {
        SocketChannel socketChannel = serviceFSSocket.get(serviceName);
        assert socketChannel != null : "unknown fs socket client for " + serviceName;

        // send get command (g) to the filesystem
        try {
            System.out.println(">> sending fuselog command ...");
            socketChannel.write(ByteBuffer.wrap("g".getBytes()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // wait for response indicating the stateDiff size
        ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
        sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
        sizeBuffer.clear();
        System.out.println(">> reading fuselog response ...");
        int numRead = 0;
        try {
            numRead = socketChannel.read(sizeBuffer);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> got " + numRead + "bytes: " + new String(sizeBuffer.array(),
                StandardCharsets.UTF_8));
        if (numRead < 8) {
            System.err.println("failed to read size of the stateDiff");
            return null;
        }
        long stateDiffSize = sizeBuffer.getLong(0);
        System.out.println(">> stateDiff size=" + stateDiffSize);

        // read all the stateDiff
        ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
        numRead = 0;
        try {
            while (numRead < stateDiffSize) {
                numRead += socketChannel.read(stateDiffBuffer);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println(">> complete reading stateDiff ...");
        String stateDiff = Base64.getEncoder().encodeToString(stateDiffBuffer.array());
        System.out.println(">> read stateDiff: " + stateDiff);

        // convert the stateDiff into String
        return stateDiff;
    }

    @Override
    public boolean applyStateDiff(String serviceName, String encodedState) {
        // TODO: directly apply stateDiff from the obtained byte[], not via
        //  the fuselog-apply program, which we currently use.

        String diffFile = this.baseDiffDirPath + serviceName + ".diff";
        String targetDir = baseMountDirPath + serviceName + "/";

        System.out.println(">> stateDiff: " + encodedState);

        // store stateDiff into an external file
        byte[] stateDiff;
        try {
            FileOutputStream outputStream = null;
            outputStream = new FileOutputStream(diffFile);
            stateDiff = Base64.getDecoder().decode(encodedState);
            outputStream.write(stateDiff);
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // preparing the shell command to apply stateDiff
        String cmd = String.format("%s %s",
                FUSELOG_APPLY_BIN_PATH, targetDir);
        Map<String, String> env = new HashMap<>();
        env.put("FUSELOG_STATEDIFF_FILE", diffFile);
        int exitCode = Shell.runCommand(cmd, true, env);
        assert exitCode == 0 : "failed to apply stateDiff with exit code " + exitCode;

        return true;
    }
}
