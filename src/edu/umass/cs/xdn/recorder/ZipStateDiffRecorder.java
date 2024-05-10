package edu.umass.cs.xdn.recorder;

import edu.umass.cs.utils.ZipFiles;
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

public class ZipStateDiffRecorder extends AbstractStateDiffRecorder {

    private static final String defaultWorkingBasePath = "/tmp/xdn/state/zip/";

    private final String baseMountDirPath;
    private final String baseSnapshotDirPath;
    private final String baseZipDirPath;

    public ZipStateDiffRecorder(String nodeID) {
        super(nodeID, defaultWorkingBasePath + nodeID + "/mnt/");

        // create working mount dir, if not exist
        // e.g., /tmp/xdn/state/zip/node1/mnt/
        this.baseMountDirPath = this.baseDirectoryPath;
        try {
            Files.createDirectories(Paths.get(this.baseMountDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create snapshot dir, if not exist
        // e.g., /tmp/xdn/state/zip/node1/snp/
        this.baseSnapshotDirPath = defaultWorkingBasePath + nodeID + "/snp/";
        try {
            Files.createDirectories(Paths.get(this.baseSnapshotDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

        // create diff dir, if not exist
        // e.g., /tmp/xdn/state/zip/node1/diff/
        this.baseZipDirPath = defaultWorkingBasePath + nodeID + "/diff/";
        try {
            Files.createDirectories(Paths.get(this.baseZipDirPath));
        } catch (IOException e) {
            System.err.println("ERROR: " + e);
            throw new RuntimeException(e);
        }

    }

    @Override
    public String getTargetDirectory(String serviceName) {
        return baseMountDirPath + serviceName + "/";
    }

    @Override
    public boolean preInitialization(String serviceName) {
        String targetDir = this.getTargetDirectory(serviceName);

        // create target mnt dir, if not exist
        // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/
        try {
            Shell.runCommand("rm -rf " + targetDir);
            Files.createDirectory(Paths.get(targetDir));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean postInitialization(String serviceName) {
        // do nothing
        return true;
    }

    @Override
    public String captureStateDiff(String serviceName) {
        String targetMountDir = baseMountDirPath + serviceName + "/";
        String targetSnpDir = baseSnapshotDirPath + serviceName + "/";
        String targetZipFile = baseZipDirPath + serviceName + ".zip";

        // remove previous snapshot, if any
        Shell.runCommand("rm -rf " + targetSnpDir);

        // remove previous snapshot zip file, if any
        Shell.runCommand("rm -rf " + targetZipFile);

        // copy the whole state
        int exitCode = Shell.runCommand(String.format("cp -a %s %s", targetMountDir, targetSnpDir));
        if (exitCode != 0) {
            throw new RuntimeException("failed to copy the state");
        }

        // archive the copied state
        ZipFiles.zipDirectory(new File(targetSnpDir), targetZipFile);

        // read the archive into byte[]
        byte[] stateDiff = null;
        try {
            stateDiff = Files.readAllBytes(Path.of(targetZipFile));
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

        return Base64.getEncoder().encodeToString(compressedStateDiff);
    }

    @Override
    public boolean applyStateDiff(String serviceName, String encodedState) {
        String targetMountDir = baseMountDirPath + serviceName + "/";
        String targetSnpDir = baseSnapshotDirPath + serviceName + "/";
        String targetZipFile = baseZipDirPath + serviceName + ".zip";

        // convert the compressed stateDiff back to byte[]
        byte[] compressedStateDiff = Base64.getDecoder().decode(encodedState);

        // decompress the stateDiff
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

        // write the stateDiff back to .zip file
        try {
            Files.write(
                    Paths.get(targetZipFile),
                    stateDiff,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.DSYNC
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // un-archive the .zip file
        ZipFiles.unzip(targetZipFile, targetSnpDir);

        // copy back the un-archived state to the mount dir
        int exitCode = Shell.runCommand(
                String.format("cp -a %s %s", targetSnpDir, targetMountDir));
        if (exitCode != 0) {
            throw new RuntimeException("failed to apply zip stateDiff");
        }

        return true;
    }
}
