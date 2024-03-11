package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.examples.noop.NoopPaxosApp;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.Objects;
import java.util.Set;

public class XDNClient extends ReconfigurableAppClientAsync<Request> {

    NoopPaxosApp npa;

    public XDNClient() throws IOException {
        super();
        this.npa = new NoopPaxosApp();
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        return new XDNRequest();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return null;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(">>>>>" + args);
        XDNClient client = new XDNClient();

        if (args.length > 0 && Objects.equals(args[0], "abc")) {
            System.out.println(client);
        }
    }

}
