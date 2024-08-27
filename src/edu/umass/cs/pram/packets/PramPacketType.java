package edu.umass.cs.pram.packets;

import edu.umass.cs.cops.packets.CopsPacketType;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.protocoltask.examples.PingPongPacket;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.util.HashMap;
import java.util.Map;

public enum PramPacketType implements IntegerPacketType {

    PRAM_PACKET(41400),

    // Client -> Entry Replica
    PRAM_READ_PACKET(41401),

    // Client -> Entry Replica
    PRAM_WRITE_PACKET(41402),

    // Entry Replica -> All Replicas
    PRAM_WRITE_AFTER_PACKET(41403);

    private static final Map<Integer, PramPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (PramPacketType type : PramPacketType.values()) {
            if (!PramPacketType.numbers.containsKey(type.number)) {
                PramPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    PramPacketType(int number) {this.number = number;}

    @Override
    public int getInt() {
        return number;
    }

    public static final IntegerPacketTypeMap<PramPacketType> intToType = new IntegerPacketTypeMap<PramPacketType>(
            PramPacketType.values());
}
