package edu.umass.cs.reconfiguration.interfaces;

import java.net.InetAddress;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ActiveReplica;

/**
 * A minimal interface defining active replica server functions. This
 * interface is implemented by {@link ActiveReplica}.
 * 
 * @author gaozy
 *
 */
public interface ActiveReplicaFunctions {
	
	/**
	 * @param request 
	 * @param callback 
	 * @return true if request is executed successfully
	 */
	public boolean handRequestToAppForHttp(Request request, ExecutedCallback callback);

	/**
	 * @param request
	 * @param addr
	 */
	public void updateDemandStatsFromHttp(Request request, InetAddress addr);
}
