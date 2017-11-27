package edu.umass.cs.gigapaxos.examples.adder;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.noop.NoopApp;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;

import java.util.Set;

/**
 * @author arun
 *
 */
public class StatefulAdderApp implements Replicable {

	protected int total = 0;

	@Override
	public boolean execute(Request request) {
		// execute request here
		if (request instanceof RequestPacket) {
			String requestValue = ((RequestPacket) request).requestValue;
			try {
				total += Integer.valueOf(requestValue);
			} catch(NumberFormatException nfe) {
				nfe.printStackTrace();
			}
			// set response if request instanceof InterfaceClientRequest
			((RequestPacket) request).setResponse("total="+this.total);
		}
		else System.err.println("Unknown request type: " + request.getRequestType());
		return true;
	}

	@Override
	public boolean execute(Request request,
						   boolean doNotReplyToClient) {
		// Identical to above unless app manages its own messaging, i.e.,
		// it doesn't use setResponse(.), and doNotReplyToClient is true.
		return this.execute(request);
	}

	@Override
	public String checkpoint(String name) {
		// should return entire state here
		return this.total+"";
	}

	@Override
	public boolean restore(String name, String state) {
		// Should update checkpoint state here for name, but
		// we only expect one default service name here.
		assert(name.equals(PaxosConfig.getDefaultServiceName())) : name;

		// null state is equivalent to reinitialization
		if(state==null || state.equals(Config.getGlobalString(PaxosConfig.PC
				.DEFAULT_NAME_INITIAL_STATE)))
			this.total = 0;
		else
			try {
				int number = Integer.valueOf(state);
				this.total = number;
			} catch(NumberFormatException nfe) {
				nfe.printStackTrace();
			}
		return true;
	}

	/**
	 * Needed only if app uses request types other than RequestPacket. Refer
	 * {@link NoopApp} for a more detailed example.
	 */
	@Override
	public Request getRequest(String stringified)
			throws RequestParseException {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * Needed only if app uses request types other than RequestPacket. Refer
	 * {@link NoopApp} for a more detailed example.
	 */
	@Override
	public Set<IntegerPacketType> getRequestTypes() {
		// TODO Auto-generated method stub
		return null;
	}
}
