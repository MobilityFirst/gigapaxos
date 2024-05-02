package edu.umass.cs.primarybackup.examples;

import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PrimaryEpoch;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MonotonicApp is a simple application, with no sharding i.e, only single object/service name,
 * that records a sequence of monotonically increasing numbers. For example, the app records these
 * numbers below:
 *  [1, 3, 10, 99, 1001, 3011]
 * <p>
 * The app can receive two kind of requests: GenerateNumber and GetSequence, which respectively,
 * generate a strictly increasing number, and return the overall sequence of the previously
 * generated numbers. Correct implementation of replication should ensure the invariant that ensures
 * the monotonically increasing numbers, despite reconfiguration (e.g, change of replica groups,
 * change of primary node, some nodes crashed).
 */
public class MonotonicApp implements Replicable, Reconfigurable, BackupableApplication {

    /**
     * The followings are the application state: the sequence.
     */
    protected record Number(Integer timestamp, Integer number) {
    }

    protected final List<Number> sequence = new ArrayList<>();

    private void start(int port) {
        ServerSocket serverSocket;
        Socket clientSocket;
        PrintWriter out;
        BufferedReader in;

        try {
            serverSocket = new ServerSocket(port);
            System.out.println(" waiting for a client ...");
            clientSocket = serverSocket.accept();
            System.out.printf(" client is accepted (%s)\n",
                    clientSocket.getRemoteSocketAddress().toString());
            out = new PrintWriter(clientSocket.getOutputStream(), true);
            in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));

            boolean isExit = false;
            while (!isExit) {
                String command = in.readLine();
                System.out.printf("Command '%s' is accepted.\n", command);

                switch (command) {
                    case MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND -> {
                        int num = handleGenerateNumberRequest();
                        out.write("Generated number: " + num + "\n");
                    }
                    case MonotonicAppRequest.MONOTONIC_APP_GET_SEQUENCE_COMMAND -> {
                        String seqStr = handleGetSequenceRequest();
                        out.write("Sequence: " + seqStr + "\n");
                    }
                    case MonotonicAppRequest.MONOTONIC_APP_EXIT_COMMAND -> {
                        isExit = true;
                        in.close();
                        out.close();
                        clientSocket.close();
                        serverSocket.close();
                    }
                    case null -> {
                        // do nothing
                    }
                    default -> {
                        String help = String.format(
                                "Unknown command '%s'! use '%s', '%s', or %s.\n",
                                command,
                                MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND,
                                MonotonicAppRequest.MONOTONIC_APP_GET_SEQUENCE_COMMAND,
                                MonotonicAppRequest.MONOTONIC_APP_EXIT_COMMAND);
                        out.write(help);
                    }
                }
                out.flush();
            }

        } catch (IOException e) {
            System.out.println("Exception: " + e);
            throw new RuntimeException(e);
        }
    }

    private synchronized int handleGenerateNumberRequest() {
        int n = 0;
        if (!sequence.isEmpty()) {
            Number last = sequence.getLast();
            n = last.number + getRandomNumberBetween(5, 1);
        }
        Number pair = new Number((int) (System.currentTimeMillis()), n);
        sequence.add(pair);
        return n;
    }

    private int getRandomNumberBetween(int max, int min) {
        return (int) ((Math.random() * (max - min)) + min);
    }

    private synchronized String handleGetSequenceRequest() {
        StringBuilder seqStr = new StringBuilder();
        int size = sequence.size();
        for (Number n : sequence) {
            seqStr.append(n.number);
            size = size - 1;
            if (size > 0) {
                seqStr.append(", ");
            }
        }
        return seqStr.toString();
    }

    public static void main(String[] args) {
        int port = 5555;
        MonotonicApp server = new MonotonicApp();
        System.out.println("Starting monotonic app in ':" + port + "' ...");
        server.start(port);
    }

    //==============================================================================================
    // Replicable implementation
    //==============================================================================================

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        assert stringified != null &&
                stringified.startsWith(MonotonicAppRequest.SERIALIZED_PREFIX) :
                "MonotonicApp: Unknown request " + stringified;
        return MonotonicAppRequest.createFromString(stringified);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(MonotonicAppRequest.MonotonicAppRequestType);
        return types;
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        if (!(request instanceof MonotonicAppRequest r)) {
            System.out.printf("Error, unknown request of class %s\n",
                    request.getClass().getSimpleName());
            return true;
        }

        String command = r.getCommand();
        switch (command) {
            case MonotonicAppRequest.MONOTONIC_APP_GEN_NUMBER_COMMAND -> {
                int num = handleGenerateNumberRequest();
                r.setResponse(String.valueOf(num));
            }
            case MonotonicAppRequest.MONOTONIC_APP_GET_SEQUENCE_COMMAND -> {
                String sequenceStr = handleGetSequenceRequest();
                r.setResponse(sequenceStr);
            }
            default -> {
                System.out.printf("Unknown command %s\n", command);
            }
        }

        return true;
    }

    @Override
    public String checkpoint(String name) {
        System.out.printf("MonotonicApp checkpoint name=%s\n", name);
        StringBuilder stateSnapshot = new StringBuilder();
        for (Number n : sequence) {
            stateSnapshot.append(String.format("%d:%d,", n.timestamp, n.number));
        }
        return stateSnapshot.toString();
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.printf("MonotonicApp restore name=%s state=%s\n", name, state);
        if (state == null || state.isEmpty()) {
            this.sequence.clear();
            return true;
        }

        this.sequence.clear();
        String[] numberStrings = state.split("\\.");
        for (String numberStr : numberStrings) {
            String[] numberRaw = numberStr.split(":");
            int timestamp = Integer.parseInt(numberRaw[0]);
            int number = Integer.parseInt(numberRaw[1]);
            sequence.add(new Number(timestamp, number));
        }
        return true;
    }



    //==============================================================================================
    // BackupableApplication implementation.
    //
    // Capturing statediff must be called after execution, it is simply getting the last number
    // from sequence. Applying statediff then simply adding number at the end of sequence.
    //==============================================================================================
    @Override
    public String captureStatediff(String serviceName) {
        Number last = null;
        synchronized (sequence) {
            last = sequence.getLast();
        }
        if (last == null) {
            return "";
        }
        return String.format("%d:%d", last.timestamp, last.number);
    }

    @Override
    public boolean applyStatediff(String serviceName, String statediff) {
        System.out.println(">> applying stateDiff: " + statediff);
        String[] lastNumber = statediff.split(":");
        int timestamp = Integer.parseInt(lastNumber[0]);
        int number = Integer.parseInt(lastNumber[1]);
        synchronized (sequence) {
            sequence.add(new Number(timestamp, number));
        }
        return true;
    }

    private record MonotonicAppStopRequest(String serviceName, int placementEpoch)
            implements ReconfigurableRequest {

        @Override
        public IntegerPacketType getRequestType() {
            return ReconfigurableRequest.STOP;
        }

        @Override
        public String getServiceName() {
            return this.serviceName;
        }

        @Override
        public int getEpochNumber() {
            return this.placementEpoch;
        }

        @Override
        public boolean isStop() {
            return true;
        }
    }

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return new MonotonicAppStopRequest(name, epoch);
    }

    @Override
    public String getFinalState(String name, int epoch) {
        return null;
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {

    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        return false;
    }

    @Override
    public Integer getEpoch(String name) {
        return 0;
    }

}
