package edu.umass.cs.xdn.utils;

import java.io.IOException;

public class Utils {
    public static int getUid() {
        int uid = 0;

        String getUidCommand = "id -u";
        ProcessBuilder pb = new ProcessBuilder(getUidCommand.split(" "));
        try {
            Process p = pb.start();
            int ret = p.waitFor();
            if (ret != 0) {
                return uid;
            }
            String output = new String(p.getInputStream().readAllBytes());
            output = output.replace("\n", "");
            uid = Integer.parseInt(output);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return uid;
    }

    public static int getGid() {
        int gid = 0;

        String getGidCommand = "id -g";
        ProcessBuilder pb = new ProcessBuilder(getGidCommand.split(" "));
        try {
            Process p = pb.start();
            int ret = p.waitFor();
            if (ret != 0) {
                return gid;
            }
            String output = new String(p.getInputStream().readAllBytes());
            output = output.replace("\n", "");
            gid = Integer.parseInt(output);
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return gid;
    }
}
