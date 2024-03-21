package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

/**
 * XDNRequest is an umbrella class that holds all Requests handled by XDN Application and
 * Replica Coordinator. All the Request's types are available in {@link XDNRequestType}.
 * <p>
 * Unlike Packet, a Request may or may not need a Response. For example, {@link XDNHttpRequest}
 * has an attribute to store a Response (via {@link XDNHttpRequest#setHttpResponse(HttpResponse)}),
 * but the Response itself can still be Null.
 * <p>
 * Currently, there are three kind of Requests supported.
 *                            ┌────────────┐
 *         ┌─────────────────►│ XDNRequest │◄───────────────┐
 *         │                  └────────────┘                │
 *         │                        ▲                       │
 *         │                        │                       │
 * ┌───────┴───────┐ ┌──────────────┴──────┐ ┌──────────────┴─────────┐
 * │ XDNHttpRequest│ │XDNHttpForwardRequest│ │XDNStatediffApplyRequest│
 * └───────────────┘ └─────────────────────┘ └────────────────────────┘
 *
 */
public abstract class XDNRequest implements ReplicableRequest {

    // SERIALIZED_PREFIX is used as prefix of the serialized (string) version of XDNRequest,
    // otherwise Gigapaxos will detect it as JSONPacket and handle it incorrectly.
    public static final String SERIALIZED_PREFIX = "xdn:";

}
