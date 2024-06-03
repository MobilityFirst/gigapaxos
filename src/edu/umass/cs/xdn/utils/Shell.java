package edu.umass.cs.xdn.utils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class Shell {

    public static int runCommand(String command, boolean isSilent,
                                 Map<String, String> environmentVariables) {
        try {
            // prepare to start the command
            ProcessBuilder pb = new ProcessBuilder(command.split("\\s+"));
            if (isSilent) {
                pb.redirectOutput(ProcessBuilder.Redirect.DISCARD);
                pb.redirectError(ProcessBuilder.Redirect.DISCARD);
            }

            if (environmentVariables != null) {
                Map<String, String> processEnv = pb.environment();
                processEnv.putAll(environmentVariables);
            }

            if (!isSilent) {
                System.out.println("command: " + command);
                if (environmentVariables != null) {
                    System.out.println(environmentVariables.toString());
                }
            }

            // run the command as a new OS process
            Process process = pb.start();

            // print out the output in stderr, if needed
            if (!isSilent) {
                InputStream inputStream = process.getInputStream();
                String output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1);
                if (!output.isEmpty())
                    System.out.println("output:\n" + output);

                InputStream errStream = process.getErrorStream();
                String err = new String(errStream.readAllBytes(), StandardCharsets.ISO_8859_1);
                if (!err.isEmpty())
                    System.out.println("error:\n" + err);
            }

            int exitCode = process.waitFor();

            if (!isSilent) {
                System.out.println("exit code: " + exitCode);
            }

            return exitCode;
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public static int runCommand(String command, boolean isSilent) {
        return runCommand(command, isSilent, null);
    }

    public static int runCommand(String command) {
        return runCommand(command, true);
    }

}
