package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.HashMap;

public enum XDNRequestType implements IntegerPacketType {

    XDN_SERVICE_HTTP_REQUEST(31300),
    XDN_HTTP_FORWARD_REQUEST(31301),
    XDN_STATEDIFF_APPLY_REQUEST(31302);

    private static final HashMap<Integer, XDNRequestType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (XDNRequestType type : XDNRequestType.values()) {
            if (!XDNRequestType.numbers.containsKey(type.number)) {
                XDNRequestType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    XDNRequestType(int t) {
        this.number = t;
    }

    @Override
    public int getInt() {
        return this.number;
    }
}
