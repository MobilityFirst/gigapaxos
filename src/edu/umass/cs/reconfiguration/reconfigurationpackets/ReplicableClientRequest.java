package edu.umass.cs.reconfiguration.reconfigurationpackets;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParserBytes;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

/**
 * @author arun
 *
 */
public class ReplicableClientRequest extends JSONPacket implements
		ReplicableRequest, ClientRequest, ReconfigurableRequest, Byteable {

	private static enum Keys {
		QID, QV, COORD
	};

	final Request request;
	final long requestID;
	boolean needsCoordination;
	final byte[] requestBytes;

	// not serialized
	private NIOHeader nioHeader;

	/**
	 * @param request
	 * @return Wrapped app request from request.
	 */
	public static ReplicableClientRequest wrap(Request request) {
		return new ReplicableClientRequest(request);
	}

	/**
	 * @param request
	 * @param requestID
	 * @return Wrapped app request from request.
	 */
	public static ReplicableClientRequest wrap(Request request, long requestID) {
		return new ReplicableClientRequest(request, requestID);
	}

	/**
	 * @param request
	 * @param coord
	 *            If true, the request will be coordinated, else it will not be
	 *            coordinated. If the {@code coord} flag is specified, it
	 *            determines the coordination mode even if the underlying
	 *            {@code request} is a {@link ReplicableRequest} and its
	 *            {@link ReplicableRequest#needsCoordination()} method returns a
	 *            conflicting value.
	 * 
	 * @return WrappedAppRequest from request.
	 */
	public static ReplicableClientRequest wrap(Request request, boolean coord) {
		return new ReplicableClientRequest(request, coord);
	}

	/**
	 * 
	 */
	public static final boolean DEFAULT_COORDINATION_MODE = false;

	ReplicableClientRequest(Request request) {
		this(
				request,
				request instanceof ReplicableRequest ? ((ReplicableRequest) request)
						.needsCoordination() : DEFAULT_COORDINATION_MODE);
	}

	ReplicableClientRequest(Request request, boolean coord) {
		this(request,
				request instanceof ClientRequest ? ((ClientRequest) request)
						.getRequestID()
						: (long) (Math.random() * Long.MAX_VALUE), coord);
	}

	ReplicableClientRequest(Request request, long requestID) {
		this(
				request,
				requestID,
				request instanceof ReplicableRequest ? ((ReplicableRequest) request)
						.needsCoordination() : DEFAULT_COORDINATION_MODE);
	}

	ReplicableClientRequest(Request request, long requestID, boolean coord) {
		super(request.getRequestType());
		this.request = request;
		this.requestID = requestID;
		this.requestBytes = null;
		this.needsCoordination = coord;
	}

	ReplicableClientRequest(ByteBuffer bbuf, NIOHeader header, Object parser) {
		super(new IntegerPacketType() {
			@Override
			public int getInt() {
				return bbuf.getInt();
			}
		});
		this.requestID = bbuf.getLong();
		this.needsCoordination = bbuf.get() == 0 ? false : true;
		this.requestBytes = Arrays.copyOfRange(bbuf.array(), 12,
				bbuf.array().length);
		this.request = this.parse(this.requestBytes, header, parser);
	}

	private Request parse(byte[] buf, NIOHeader header, Object parser) {
		return parser instanceof AppRequestParserBytes ? this.parse(buf,
				header, (AppRequestParserBytes) parser) : this.parse(buf,
				header, (AppRequestParser) parser);
	}

	private Request parse(byte[] buf, NIOHeader header,
			AppRequestParserBytes parser) {
		try {
			return parser.getRequest(buf, header);
		} catch (RequestParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	private Request parse(byte[] buf, NIOHeader header, AppRequestParser parser) {
		try {
			return parser.getRequest(new String(buf, CHARSET));
		} catch (UnsupportedEncodingException | RequestParseException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * @param buf
	 * @param header
	 * @param parser
	 */
	public ReplicableClientRequest(byte[] buf, NIOHeader header,
			AppRequestParserBytes parser) {
		this(ByteBuffer.wrap(buf), header, parser);
	}

	/**
	 * @param buf
	 * @param parser
	 */
	public ReplicableClientRequest(byte[] buf, AppRequestParser parser) {
		this(ByteBuffer.wrap(buf), null, parser);
	}

	ReplicableClientRequest(JSONObject json) throws JSONException,
			UnsupportedEncodingException {
		this(json, null);
	}

	/**
	 * @param json
	 * @param unstringer
	 * @throws JSONException
	 * @throws UnsupportedEncodingException
	 */
	public ReplicableClientRequest(JSONObject json, Stringifiable<?> unstringer)
			throws JSONException, UnsupportedEncodingException {
		// ignore unstringer
		super(json);
		this.requestID = json.getLong(Keys.QID.toString());
		this.request = null;
		this.needsCoordination = json.has(Keys.COORD.toString()) ? json
				.getBoolean(Keys.COORD.toString()) : false;
		this.requestBytes = json.getString(Keys.QV.toString())
				.getBytes(CHARSET);
	}

	/**
	 * @param parser
	 * @return App request
	 * @throws UnsupportedEncodingException
	 * @throws RequestParseException
	 */
	public Request getRequest(AppRequestParser parser)
			throws UnsupportedEncodingException, RequestParseException {
		return parser.getRequest(new String(this.requestBytes, CHARSET));
	}
	/**
	 * @return The underlying app request.
	 */
	public Request getRequest() {
		return this.request;
	}

	/**
	 * @param parser
	 * @param header
	 * @return App request
	 * @throws UnsupportedEncodingException
	 * @throws RequestParseException
	 */
	public Request getRequest(AppRequestParserBytes parser, NIOHeader header)
			throws UnsupportedEncodingException, RequestParseException {
		return parser.getRequest(this.requestBytes, header);
	}

	/**
	 * @param parser
	 * @return App request
	 * @throws UnsupportedEncodingException
	 * @throws RequestParseException
	 */
	public Request getRequest(AppRequestParserBytes parser)
			throws UnsupportedEncodingException, RequestParseException {
		return this.getRequest(parser, this.nioHeader);
	}

	@Override
	public IntegerPacketType getRequestType() {
		return ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST;
	}

	@Override
	public String getServiceName() {
		return this.request.getServiceName();
	}

	@Override
	public long getRequestID() {
		return this.requestID;
	}

	@Override
	public int getEpochNumber() {
		return request instanceof ReconfigurableRequest ? ((ReconfigurableRequest) this.request)
				.getEpochNumber() : 0;
	}

	@Override
	public boolean isStop() {
		return request instanceof ReconfigurableRequest ? ((ReconfigurableRequest) this.request)
				.isStop() : false;
	}

	@Override
	public ClientRequest getResponse() {
		return request instanceof ClientRequest ? ((ClientRequest) this.request)
				.getResponse() : null;
	}

	@Override
	public boolean needsCoordination() {
		return this.needsCoordination;
	}

	@Override
	protected JSONObject toJSONObjectImpl() throws JSONException {
		JSONObject json = new JSONObject();
		json.put(Keys.QID.toString(), this.requestID);
		json.put(Keys.COORD.toString(), this.needsCoordination);
		json.put(Keys.QV.toString(), this.request.toString());
		return json;
	}

	/**
	 * Default charset used to encode this packet to bytes.
	 */
	public final String CHARSET = "ISO-8859-1";

	@Override
	public byte[] toBytes() {
		byte[] requestBytes;
		try {
			requestBytes = this.request instanceof Byteable ? ((Byteable) this.request)
					.toBytes() : this.request.toString().getBytes(CHARSET);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
			return null;
		}
		byte[] buf = new byte[
		// type
		Integer.BYTES
		// ID
				+ Long.BYTES
				// needsCoordination
				+ 1
				// request bytes
				+ requestBytes.length];

		return ByteBuffer.wrap(buf)
		// type
				.putInt(this.getRequestType().getInt())
				// ID
				.putLong(this.requestID)
				// needsCoordination
				.put(this.needsCoordination ? (byte) 1 : (byte) 0)
				// request bytes
				.put(requestBytes).array();
	}

	/**
	 * @return Request as String.
	 */
	public String getRequestAsString() {
		try {
			return this.requestBytes != null ? new String(this.requestBytes,
					CHARSET) : this.request.toString();
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return this.request != null ? this.request.toString() : null;
	}
}
