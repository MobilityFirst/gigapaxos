package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;

public record XDNRequestAndCallback(XDNRequest request, ExecutedCallback callback) {

}
