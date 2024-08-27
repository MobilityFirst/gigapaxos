package edu.umass.cs.cops.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.HashMap;
import java.util.Map;

public enum CopsPacketType implements IntegerPacketType {

    COPS_PACKET(41300),

    // Entry Replica -> All Replicas
    COPS_PUT_AFTER_PACKET(41301),

    // Client -> Entry Replica
    COPS_APP_REQUEST_PACKET(41302);

    private static final Map<Integer, CopsPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (CopsPacketType type : CopsPacketType.values()) {
            if (!CopsPacketType.numbers.containsKey(type.number)) {
                CopsPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    CopsPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }
}
