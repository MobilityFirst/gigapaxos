package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;

public class XDNHttpRequest extends XDNRequest {

    /**
     * All the serialized XDNHttpRequest starts with "xdn:31300:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_SERVICE_HTTP_REQUEST.getInt());

    private static final String XDN_HTTP_REQUEST_ID_HEADER = "XDN-Request-ID";

    private final long requestID;
    private final String serviceName;
    private final HttpRequest httpRequest;
    private final HttpContent httpRequestContent;
    private HttpResponse httpResponse;

    public XDNHttpRequest(String serviceName, HttpRequest httpRequest, HttpContent httpRequestContent) {
        assert serviceName != null && httpRequest != null && httpRequestContent != null;
        this.serviceName = serviceName;
        this.httpRequest = httpRequest;
        this.httpRequestContent = httpRequestContent;

        Long inferredRequestID = XDNHttpRequest.inferRequestID(this.httpRequest);
        if (inferredRequestID != null) {
            this.requestID = inferredRequestID;
        } else {
            this.requestID = System.currentTimeMillis();
        }
        this.httpRequest.headers().set(XDN_HTTP_REQUEST_ID_HEADER, this.requestID);
    }

    // The service's name is encoded in the request header.
    // For example, the service name is 'hello' for these cases:
    // - request with "XDN: hello" in the header.
    // - request with "Host: hello.abc.xdn.io:80" in the header.
    // return null if service's name cannot be inferred
    public static String inferServiceName(HttpRequest httpRequest) {
        assert httpRequest != null;

        // case-1: encoded in the XDN header (e.g., XDN: alice-book-catalog)
        String xdnHeader = httpRequest.headers().get("XDN");
        if (xdnHeader != null && !xdnHeader.isEmpty()) {
            return xdnHeader;
        }

        // case-2: encoded in the required Host header (e.g., Host: alice-book-catalog.xdn.io)
        String hostStr = httpRequest.headers().get(HttpHeaderNames.HOST);
        if (hostStr == null || hostStr.isEmpty()) {
            return null;
        }
        String reqServiceName = hostStr.split("\\.")[0];
        if (!reqServiceName.isEmpty()) {
            return reqServiceName;
        }

        return null;
    }

    // In general, we infer the HTTP request ID based on these headers:
    // (1) `ETag`, (2) `X-Request-ID`, or (3) `XDN-Request-ID`, in that order.
    // If the request does not contain those header, null will be returned.
    // Later, the newly generated request ID should be stored in the header
    // with `XDN-Request-ID` key (check the constructor of XDNHttpRequest).
    public static Long inferRequestID(HttpRequest httpRequest) {
        assert httpRequest != null;

        // case-1: encoded as Etag header
        String etag = httpRequest.headers().get(HttpHeaderNames.ETAG);
        if (etag != null) {
            return (long) Objects.hashCode(etag);
        }

        // case-2: encoded as X-Request-ID header
        String xRequestID = httpRequest.headers().get("X-Request-ID");
        if (xRequestID != null) {
            return (long) Objects.hashCode(xRequestID);
        }

        // case-3: encoded as XDN-Request-ID header
        String xdnReqID = httpRequest.headers().get("XDN-Request-ID");
        if (xdnReqID != null) {
            return Long.valueOf(xdnReqID);
        }

        return null;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_SERVICE_HTTP_REQUEST;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNHttpRequest that = (XDNHttpRequest) o;
        return this.serviceName.equals(that.serviceName) &&
                this.httpRequest.equals(that.httpRequest) &&
                this.httpRequestContent.equals(that.httpRequestContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.serviceName, this.httpRequest, this.httpRequestContent);
    }

    public HttpRequest getHttpRequest() {
        return httpRequest;
    }

    public HttpContent getHttpRequestContent() {
        return httpRequestContent;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        this.httpResponse = httpResponse;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("protocolVersion", httpRequest.protocolVersion().toString());
            json.put("method", httpRequest.method().toString());
            json.put("uri", httpRequest.uri());
            JSONArray headerJsonArray = new JSONArray();
            Iterator<Map.Entry<String, String>> it = httpRequest.headers().iteratorAsString();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
            }
            json.put("headers", headerJsonArray);
            json.put("content", httpRequestContent.content().toString(StandardCharsets.ISO_8859_1));
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static XDNHttpRequest createFromString(String stringified) {
        if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        stringified = stringified.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(stringified);

            // prepare the deserialized variables
            String httpProtocolVersion = json.getString("protocolVersion");
            String httpMethod = json.getString("method");
            String httpURI = json.getString("uri");
            String httpContent = json.getString("content");

            // handle array of header
            JSONArray headerJSONArr = json.getJSONArray("headers");
            HttpHeaders httpHeaders = new DefaultHttpHeaders();
            for (int i = 0; i < headerJSONArr.length(); i++) {
                String headerEntry = headerJSONArr.getString(i);
                String headerKey = headerEntry.split(":")[0];
                String headerVal = headerEntry.substring(headerKey.length() + 1);
                httpHeaders.add(headerKey, headerVal);
            }

            // init request and content, then combine them into XDNHttpRequest
            HttpRequest req = new DefaultHttpRequest(
                    HttpVersion.valueOf(httpProtocolVersion),
                    HttpMethod.valueOf(httpMethod),
                    httpURI,
                    httpHeaders);
            HttpContent reqContent = new DefaultHttpContent(
                    Unpooled.copiedBuffer(httpContent, StandardCharsets.ISO_8859_1));
            String serviceName = XDNHttpRequest.inferServiceName(req);
            return new XDNHttpRequest(serviceName, req, reqContent);
        } catch (JSONException e) {
            e.printStackTrace();
            return null;
        }
    }

}
