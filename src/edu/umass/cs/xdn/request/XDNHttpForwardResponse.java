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

public class XDNHttpForwardResponse extends XDNRequest {

    /**
     * All the serialized XDNHttpForwardResponse starts with "xdn:31302:"
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_HTTP_FORWARD_RESPONSE.getInt());

    private long requestID;
    private final String serviceName;
    private final HttpResponse httpResponse;

    public XDNHttpForwardResponse(long requestID, String serviceName, HttpResponse httpResponse) {
        this.requestID = requestID;
        this.serviceName = serviceName;
        this.httpResponse = httpResponse;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_HTTP_FORWARD_RESPONSE;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public long getRequestID() {
        return requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public HttpResponse getHttpResponse() {
        return httpResponse;
    }

    @Override
    public String toString() {
        try {
            JSONObject json = new JSONObject();
            json.put("id", this.requestID);
            json.put("name", this.serviceName);
            JSONArray headerJsonArray = new JSONArray();
            DefaultFullHttpResponse fullHttpResponse = (DefaultFullHttpResponse) this.httpResponse;
            Iterator<Map.Entry<String, String>> it = fullHttpResponse.headers().iteratorAsString();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
            }
            json.put("protocolVersion", fullHttpResponse.protocolVersion().toString());
            json.put("status", fullHttpResponse.status().code());
            json.put("headers", headerJsonArray);
            json.put("content", fullHttpResponse.content().toString(StandardCharsets.ISO_8859_1));
            return SERIALIZED_PREFIX + json.toString();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    public static XDNHttpForwardResponse createFromString(String stringified) {
        if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        stringified = stringified.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(stringified);

            // prepare the deserialized variable
            long requestID = json.getLong("id");
            String serviceName = json.getString("name");
            String httpProtocolVersion = json.getString("protocolVersion");
            int httpResponseStatus = json.getInt("status");
            String httpResponseContent = json.getString("content");

            // handle array of header
            JSONArray headerJSONArr = json.getJSONArray("headers");
            HttpHeaders httpHeaders = new DefaultHttpHeaders();
            for (int i = 0; i < headerJSONArr.length(); i++) {
                String headerEntry = headerJSONArr.getString(i);
                String headerKey = headerEntry.split(":")[0];
                String headerVal = headerEntry.substring(headerKey.length() + 1);
                httpHeaders.add(headerKey, headerVal);
            }
            HttpHeaders httpTrailingHeaders = new DefaultHttpHeaders();

            // init request and content, then combine them into XDNHttpRequest
            HttpResponse resp = new DefaultFullHttpResponse(
                    HttpVersion.valueOf(httpProtocolVersion),
                    HttpResponseStatus.valueOf(httpResponseStatus),
                    Unpooled.copiedBuffer(httpResponseContent, StandardCharsets.ISO_8859_1),
                    httpHeaders,
                    httpTrailingHeaders
            );
            return new XDNHttpForwardResponse(requestID, serviceName, resp);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
