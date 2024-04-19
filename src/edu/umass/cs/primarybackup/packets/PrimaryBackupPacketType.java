package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.HashMap;
import java.util.Map;

public enum PrimaryBackupPacketType implements IntegerPacketType {

    // Primary -> All Replicas
    PB_START_EPOCH_PACKET(35400),

    // Client -> Entry Replica
    PB_REQUEST_PACKET(35401),

    // Primary -> Entry Replica
    PB_RESPONSE_PACKET(35402),

    // Entry Replica -> Primary
    PB_FORWARDED_REQUEST_PACKET(35403),

    // Client -> Entry Replica
    PB_CHANGE_PRIMARY_PACKET(35404),

    // Primary -> All Replicas
    PB_STATE_DIFF_PACKET(35405);

    private static final Map<Integer, PrimaryBackupPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (PrimaryBackupPacketType type : PrimaryBackupPacketType.values()) {
            if (!PrimaryBackupPacketType.numbers.containsKey(type.number)) {
                PrimaryBackupPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    PrimaryBackupPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }
}
