package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.utils.Shell;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterOutputStream;

public class RsyncStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String RSYNC_BIN_PATH = "/usr/bin/rsync";
    private static final String defaultWorkingBasePath = "/tmp/xdn/state/rsync/";

    private final String baseMountDirPath;
    private final String baseSnapshotDirPath;
    private final String baseDiffDirPath;

    public RsyncStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/mnt/");
        File rsync = new File(RSYNC_BIN_PATH);
        assert rsync.exists() : "rsync binary does not exist at " + RSYNC_BIN_PATH;

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath;
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("err: " + e.toString());
            throw new RuntimeException(e);
        }

        // create snapshot dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/snp/
        this.baseSnapshotDirPath = defaultWorkingBasePath + nodeID + "/snp/";
        try {
            Files.createDirectories(Paths.get(this.baseSnapshotDirPath));
        } catch (IOException e) {
            System.err.println("err: " + e.toString());
            throw new RuntimeException(e);
        }

        // create diff dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/diff/
        this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
        try {
            Files.createDirectories(Paths.get(this.baseDiffDirPath));
        } catch (IOException e) {
            System.err.println("err: " + e.toString());
            throw new RuntimeException(e);
        }
    }

    @Override
    public String getTargetDirectory(String serviceName) {
        String targetDir = baseMountDirPath + serviceName + "/";

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/
        try {
            Shell.runCommand("rm -rf " + targetDir);
            Files.createDirectory(Paths.get(targetDir));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return targetDir;
    }

    @Override
    public boolean initialize(String serviceName) {
        String targetSourceDir = baseMountDirPath + serviceName + "/";
        String targetDestDir = baseSnapshotDirPath + serviceName + "/";
        String targetDiffFile = baseDiffDirPath + serviceName + ".diff";

        Shell.runCommand("rm -rf " + targetDestDir);
        Shell.runCommand("rm -rf " + targetDiffFile);
        Shell.runCommand(String.format("cp -ar %s %s", targetSourceDir, targetDestDir));

        return true;
    }

    @Override
    public String captureStateDiff(String serviceName) {
        String targetSourceDir = baseMountDirPath + serviceName + "/";
        String targetDestDir = baseSnapshotDirPath + serviceName + "/";
        String targetDiffFile = baseDiffDirPath + serviceName + ".diff";

        String command = String.format("%s -ar --write-batch=%s %s %s",
                RSYNC_BIN_PATH, targetDiffFile, targetSourceDir, targetDestDir);
        int exitCode = Shell.runCommand(command, true);
        if (exitCode != 0) {
            throw new RuntimeException("failed to capture stateDiff");
        }

        // read diff into byte[]
        byte[] stateDiff = null;
        try {
            stateDiff = Files.readAllBytes(Path.of(targetDiffFile));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // compress stateDiff
        byte[] compressedStateDiff = null;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            DeflaterOutputStream dos = new DeflaterOutputStream(os);
            dos.write(stateDiff);
            dos.flush();
            dos.close();
            compressedStateDiff = os.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // convert the compressed stateDiff to String
        return Base64.getEncoder().encodeToString(compressedStateDiff);
    }

    @Override
    public boolean applyStateDiff(String serviceName, String encodedState) {
        String targetDir = baseMountDirPath + serviceName + "/";
        String targetDiffFile = baseDiffDirPath + serviceName + ".diff";

        Shell.runCommand("rm -rf " + targetDiffFile);

        // convert stateDiff from String back to byte[], then decompress
        byte[] compressedStateDiff = Base64.getDecoder().decode(encodedState);
        byte[] stateDiff = null;
        try {
            ByteArrayOutputStream os = new ByteArrayOutputStream();
            OutputStream ios = new InflaterOutputStream(os);
            ios.write(compressedStateDiff);
            ios.flush();
            ios.close();
            stateDiff = os.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // write stateDiff to .diff file
        try {
            Files.write(
                    Paths.get(targetDiffFile),
                    stateDiff,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DSYNC
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // apply the stateDiff inside the .diff file using rsync
        String command = String.format("%s -ar --read-batch=%s %s",
                RSYNC_BIN_PATH, targetDiffFile, targetDir);
        Shell.runCommand(command);

        return true;
    }
}
