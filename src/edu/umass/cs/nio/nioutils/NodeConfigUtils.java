package edu.umass.cs.nio.nioutils;


import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.utils.Util;

import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;

public class NodeConfigUtils {
    public static NodeConfig<String> getNodeConfig(Map<String,
            InetSocketAddress> map) {
        return new NodeConfig<String>() {

            @Override
            public String valueOf(String strValue) {
                return strValue;
            }

            @Override
            public boolean nodeExists(String id) {
                return map.containsKey(id);
            }

            @Override
            public InetAddress getNodeAddress(String id) {
                return map.containsKey(id) ? map.get(id).getAddress() : null;
            }

            @Override
            public InetAddress getBindAddress(String id) {
                return getNodeAddress(id);
            }

            @Override
            public int getNodePort(String id) {
                return map.containsKey(id) ? map.get(id).getPort() : -1;
            }

            @Override
            public Set<String> getNodeIDs() {
                return map.keySet();
            }
        };
    }

    public static NodeConfig<String> getNodeConfigFromFile(String filename,
                                                           String nodePrefix,
                                                           int offset)
            throws IOException {
        Properties props = new Properties();
        props.load(new FileInputStream(filename));
        Map<String,InetSocketAddress> map = new HashMap<String,
                InetSocketAddress>();
        for(Object prop : props.keySet()) {
            String key=prop.toString().trim();
            if(key.startsWith(nodePrefix))
                map.put(key.replace(nodePrefix, ""),
                        Util.getOffsettedAddress(
                                Util.getInetSocketAddressFromString(props.getProperty
                                        (key).trim()), offset));
        }
        return getNodeConfig(map);
    }
    public static NodeConfig<String> getNodeConfigFromFile(String filename,
                                                           String nodePrefix) throws IOException {
        return getNodeConfigFromFile(filename, nodePrefix, 0);
    }

    public static void main(String[] args) throws IOException {
        String filename = "tmp.properties", prefix="server.";
        Writer writer = new BufferedWriter(new OutputStreamWriter(new
                FileOutputStream(filename)));
        writer.write(prefix+"1 = localhost:2134\n");
        writer.write(prefix+"2= 128.119.245.20:2135\n");
        writer.close();
        NodeConfig<?> nc = NodeConfigUtils.getNodeConfigFromFile(filename,
                "server.");
        System.out.println(nc.getNodeIDs());
        new File(filename).delete();
    }
}
