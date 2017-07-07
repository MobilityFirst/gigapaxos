package edu.umass.cs.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.regex.Pattern;

/**
 * Created by kanantharamu on 2/21/17.
 */
public class UtilServer {
    /**
     * @param filename
     * @return Contents of filename as String
     * @throws IOException
     */
    public static String readFileAsString(String filename) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filename)));
    }

    /**
     * @param key
     * @param value
     * @param filename
     * @param prefix
     * @throws IOException
     */
    public static void writeProperty(String key, String value, String filename,
            String prefix) throws IOException {
        String props = readFileAsString(filename);
        String modified = "";
        String comment = "\n# automatically modified property\n";
        // append if no prefix
        if (prefix == null
                || prefix.equals("")
                || !Pattern
                        .compile(".*\n[\\s]*" + prefix + "[^\n]*=[^\n]*\n.*",
                                Pattern.DOTALL).matcher(props).matches())
            modified = props + comment + key + "=" + value;
        // replace if property exists
        else if (Pattern
                .compile(".*\n[\\s]*" + key + "[^\n]*=[^\n]*\n.*",
                        Pattern.DOTALL).matcher(props).matches()) {
            if (value != null) // replace
                modified = props
                        .replaceAll("\n[\\s]*" + key + "[^\n]*=[^\n]*\n",
                                comment + key + "=" + value + "\n");
            else
                // comment out
                modified = props.replaceAll("\n[\\s]*" + key
                        + "[^\n]*=[^\n]*\n", comment + "#" + key + "=" + value
                        + "\n");
        }
        // add as new property after last prefix match
        else {
            String[] tokens = props.split("\n");
            boolean oneTime = false;
            // Jul 9 2017: bug fix: i > 0 -> i >= 0 
            for (int i = tokens.length - 1; i >= 0; i--)
                modified = tokens[i]
                        + "\n"
                        + (tokens[i].trim().startsWith(prefix)
                                && (!oneTime && (oneTime = true)) ? comment
                                + key + "=" + value + "\n" : "") + modified;
        }
        Files.write(Paths.get(filename), modified.getBytes());
    }
}
