package edu.umass.cs.reconfiguration.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.CONTINUE;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

import edu.umass.cs.xdn.XDNRequest;
import io.netty.channel.*;
import io.netty.handler.codec.http.*;
import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;

/**
 * An HTTP front-end for an active replica that supports interaction
 * between a http client and this front-end.
 * To use this HTTP front-end, the underlying application use the request
 * type {@link HttpActiveReplicaRequest} or a type that extends {@link HttpActiveReplicaRequest}.
 * <p>
 * Loosely based on the HTTP Snoop server example from netty
 * documentation page:
 * https://github.com/netty/netty/blob/4.1/example/src/main/java/io/netty/example/http/snoop/HttpSnoopServerHandler.java
 * <p>
 * A similar implementation to {@link HttpReconfigurator}
 * <p>
 * Example command:
 * <p>
 * curl -X POST localhost -d '{NAME:"XDNApp0", QID:0, COORD: true, QVAL: "1", type: 400}' -H "Content-Type: application/json"
 * <p>
 * Or open your browser to interact with this http front end directly
 * <p>
 * Start ActiveReplica with HttpActiveReplica:
 * java -ea -cp jars/gigapaxos-1.0.08.jar -Djava.util.logging.config.file=conf/logging.properties \
 * -Dlog4j.configuration=conf/log4j.properties -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.trustStorePassword=qwerty \
 * -Djavax.net.ssl.keyStore=conf/keyStore.jks -Djavax.net.ssl.trustStore=conf/trustStore.jks \
 * -DgigapaxosConfig=conf/xdn.local.properties -DHTTPADDR=127.0.0.1 -Dcontainer=localhost:3000 \
 * edu.umass.cs.reconfiguration.ReconfigurableNode AR0
 * <p>
 * Start HttpActiveReplica alone:
 * java -ea -cp jars/gigapaxos-1.0.08.jar -DHTTPADDR=127.0.0.1 -Dcontainer=localhost:3000 \
 * edu.umass.cs.reconfiguration.http.HttpActiveReplica
 *
 * @author gaozy
 */
public class HttpActiveReplica {

    private static final Logger log = ReconfigurationConfig.getLogger();

    private final static int NUM_BOSS_THREADS = 10;

    private final static int DEFAULT_HTTP_PORT = 8080;

    private final static String DEFAULT_HTTP_ADDR = "localhost";

    private final static String HTTP_ADDR_ENV_KEY = "HTTPADDR";

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final Channel channel;

    // FIXME: used to indicate whether a single outstanding request has been executed, might go wrong when there are multiple outstanding requests
    static boolean finished;

    /**
     * @param arf
     * @param ssl
     * @throws CertificateException
     * @throws SSLException
     * @throws InterruptedException
     */
    public HttpActiveReplica(ActiveReplicaFunctions arf,
                             boolean ssl) throws CertificateException, SSLException, InterruptedException {
        this(arf, null, ssl);
    }

    /**
     * @param arf
     * @param sockAddr
     * @param ssl
     * @throws CertificateException
     * @throws SSLException
     * @throws InterruptedException
     */
    public HttpActiveReplica(ActiveReplicaFunctions arf,
                             InetSocketAddress sockAddr, boolean ssl)
            throws CertificateException, SSLException, InterruptedException {

//        if (sockAddr.getPort() == 2300) {
//            sockAddr = new InetSocketAddress(sockAddr.getAddress(), 80);
//        }

        // Configure SSL.
        final SslContext sslCtx;
        if (ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(),
                    ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        /**
         *  Configure the netty ServerBootstrap
         */
        bossGroup = new NioEventLoopGroup(NUM_BOSS_THREADS);
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .handler(new LoggingHandler(LogLevel.INFO))
                    .childHandler(
                            new HttpActiveReplicaInitializer(arf, sslCtx)
                    );

            if (sockAddr == null) {

                String addr = DEFAULT_HTTP_ADDR;
                int port = DEFAULT_HTTP_PORT;

                if (System.getProperty(HTTP_ADDR_ENV_KEY) != null) {
                    addr = System.getProperty(HTTP_ADDR_ENV_KEY);
                }
                sockAddr = new InetSocketAddress(addr, port);
            }

            channel = b.bind(sockAddr).sync().channel();

            log.log(Level.INFO, "HttpActiveReplica is ready on {0}", new Object[]{sockAddr});
            System.out.println("HttpActiveReplica ready on " + sockAddr);

            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }

    }

    /**
     * Close server and workers gracefully.
     */
    public void close() {
        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
    }


    private static class HttpActiveReplicaInitializer extends
            ChannelInitializer<SocketChannel> {

        private final SslContext sslCtx;
        final ActiveReplicaFunctions arFunctions;

        HttpActiveReplicaInitializer(final ActiveReplicaFunctions arf,
                                     SslContext sslCtx) {
            this.arFunctions = arf;
            this.sslCtx = sslCtx;
        }

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            CorsConfig corsConfig = CorsConfig.withAnyOrigin().build();

            ChannelPipeline p = channel.pipeline();

            if (sslCtx != null)
                p.addLast(sslCtx.newHandler(channel.alloc()));

            p.addLast(new HttpRequestDecoder());

            // Uncomment if you don't want to handle HttpChunks.
            p.addLast(new HttpObjectAggregator(1048576));

            p.addLast(new HttpResponseEncoder());

            p.addLast(new CorsHandler(corsConfig));

            p.addLast(new HttpActiveReplicaHandler(arFunctions, channel.remoteAddress()));

        }

    }

    private static JSONObject getJSONObjectFromHttpContent(HttpContent httpContent) {
        ByteBuf content = httpContent.content();
        byte[] bytes;
        if (content.isReadable()) {
            bytes = new byte[content.readableBytes()];
            content.readBytes(bytes);
            log.log(Level.FINE, "HttpContent: {0}", new Object[]{new String(bytes)});
        } else {
            return null;
        }

        try {
            return new JSONObject(new String(bytes));

        } catch (JSONException e) {
            return new JSONObject();
        }

    }

    /**
     * The json object must contain the following keys to be a valid request:
     * {@link HttpActiveReplicaRequest.Keys} NAME, QVAL
     * <p>
     * The other fields can be filled in with default values.
     *
     * @param json
     * @return
     * @throws HTTPException
     * @throws JSONException
     */
    private static HttpActiveReplicaRequest getRequestFromJSONObject(JSONObject json) throws HTTPException, JSONException {
        if (!json.has(HttpActiveReplicaRequest.Keys.NAME.toString())) {
            throw new JSONException("missing key NAME");
        }
        if (!json.has(HttpActiveReplicaRequest.Keys.QVAL.toString())) {
            throw new JSONException("missing key QVAL");
        }

        String name = json.getString(HttpActiveReplicaRequest.Keys.NAME.toString());
        String qval = json.getString(HttpActiveReplicaRequest.Keys.QVAL.toString());

        // needsCoordination: default true
        boolean coord = json.has(HttpActiveReplicaRequest.Keys.COORD.toString()) ?
                json.getBoolean(HttpActiveReplicaRequest.Keys.COORD.toString())
                : true;

        int qid = (json.has(HttpActiveReplicaRequest.Keys.QID.toString()) ?
                json.getInt(HttpActiveReplicaRequest.Keys.QID.toString())
                : (int) (Math.random() * Integer.MAX_VALUE));

        int epoch = (json.has(HttpActiveReplicaRequest.Keys.EPOCH.toString())) ?
                json.getInt(HttpActiveReplicaRequest.Keys.EPOCH.toString())
                : 0;

        boolean stop = (json.has(HttpActiveReplicaRequest.Keys.STOP.toString())) ?
                json.getBoolean(HttpActiveReplicaRequest.Keys.STOP.toString())
                : false;


        return new HttpActiveReplicaRequest(HttpActiveReplicaPacketType.EXECUTE,
                name, qid, qval, coord, stop, epoch);
    }

    private static class HttpExecutedCallback implements ExecutedCallback {

        StringBuilder buf;
        Object lock;
        // boolean finished;

        HttpExecutedCallback(StringBuilder buf, Object lock) {
            this.buf = buf;
            this.lock = lock;
        }

        @Override
        public void executed(Request response, boolean handled) {

            buf.append("RESPONSE:\n\r");
            buf.append(response);

            synchronized (lock) {
                finished = true;
                lock.notify();
            }
        }

    }


    private static class HttpActiveReplicaHandler extends
            SimpleChannelInboundHandler<Object> {

        ActiveReplicaFunctions arFunctions;
        final InetSocketAddress senderAddr;

        private HttpRequest request;
        /**
         * Buffer that stores the response content
         */
        private final StringBuilder buf = new StringBuilder();

        HttpActiveReplicaHandler(ActiveReplicaFunctions arFunctions, InetSocketAddress addr) {
            this.arFunctions = arFunctions;
            this.senderAddr = addr;
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {

            // redirect handling to xdn, if either of these two conditions are met:
            // (1) the HttpRequest contains non-empty XDN header, or
            // (2) the HttpRequest contains Host header ending in "xdn.io".
            // Note that "Host" header is required since HTTP 1.1
            if (msg instanceof HttpRequest) {
                boolean isXDNRequest = false;

                // handle the first condition: contains XDN header
                HttpRequest httpRequest = (HttpRequest) msg;
                String xdnHeader = httpRequest.headers().get("XDN");
                if (xdnHeader != null && xdnHeader.length() > 0) {
                    isXDNRequest = true;
                }

                // handle the second condition: Host ending with "xdn.io"
                String requestHost = httpRequest.headers().get(HttpHeaderNames.HOST);
                String[] hostPort = requestHost.split(":");
                String host = hostPort[0];
                if (host.endsWith("xdn.io")) {
                    isXDNRequest = true;
                }

                if (isXDNRequest) {
                    handleReceivedXDNRequest(ctx, msg);
                    return;
                }
            }


            /**
             * Request for GigaPaxos to coordinate
             */
            HttpActiveReplicaRequest gRequest = null;
            /**
             * JSONObject to extract keys and values from http request
             */
            JSONObject json = new JSONObject();

            /**
             * This boolean is used to indicate whether the request has been retrieved.
             * If request info is retrieved from HttpRequest, then don't bother to retrieve it from
             * HttpContent. Otherwise, retrieve the info from HttpContent.
             * If we still can't retrieve the info, then the request is a Malformed request.
             */
            boolean retrieved = false;

            if (msg instanceof HttpRequest) {
                HttpRequest httpRequest = this.request = (HttpRequest) msg;
                buf.setLength(0);

                if (HttpUtil.is100ContinueExpected(httpRequest)) {
                    send100Continue(ctx);
                }

                log.log(Level.FINE, "Http server received a request with HttpRequest: {0}", new Object[]{httpRequest});

                // converting url query parameters into JSON key value pair
                Map<String, List<String>> params = (new QueryStringDecoder(httpRequest.uri())).parameters();
                if (!params.isEmpty()) {
                    for (Entry<String, List<String>> p : params.entrySet()) {
                        String key = p.getKey();
                        List<String> vals = p.getValue();
                        for (String val : vals) {
                            // put the key-value pair into json
                            json.put(key.toUpperCase(), val);
                        }
                    }
                }

                if (json != null && json.length() > 0)
                    try {
                        gRequest = getRequestFromJSONObject(json);
                        log.log(Level.INFO, "Http server retrieved an HttpActiveReplicaRequest from HttpRequest: {0}", new Object[]{gRequest});
                        retrieved = true;
                    } catch (Exception e) {
                        // ignore and do nothing if this is a malformed request
                        e.printStackTrace();
                    }
            }

            if (msg instanceof HttpContent) {
                if (!retrieved) {
                    HttpContent httpContent = (HttpContent) msg;
                    log.log(Level.INFO, "Http server received a request with HttpContent: {0}", new Object[]{httpContent});
                    if (httpContent != null) {
                        json = getJSONObjectFromHttpContent(httpContent);
                        if (json != null && json.length() > 0)
                            try {
                                gRequest = getRequestFromJSONObject(json);
                                retrieved = true;
                            } catch (Exception e) {
                                // TODO: A malformed request, we can send back the response here
                                e.printStackTrace();
                            }
                    }

                }

                if (msg instanceof LastHttpContent) {
                    if (retrieved) {
                        log.log(Level.INFO, "About to execute request: {0}", new Object[]{gRequest});
                        Object lock = new Object();
                        finished = false;
                        ExecutedCallback callback = new HttpExecutedCallback(buf, lock);

                        // execute GigaPaxos request here
                        if (arFunctions != null) {
                            log.log(Level.FINE, "App {0} executes request: {1}", new Object[]{arFunctions, request});
                            boolean handled = arFunctions.handRequestToAppForHttp(
                                    (gRequest.needsCoordination()) ? ReplicableClientRequest.wrap(gRequest) : gRequest,
                                    callback);

                            synchronized (lock) {
                                while (!finished) {
                                    try {
                                        lock.wait(100);
                                    } catch (InterruptedException e) {

                                    }
                                }
                            }

                            /**
                             *  If the request has been handled properly, then send demand profile to RC.
                             *  This logic follows the design of (@link ActiveReplica}.
                             */
                            if (handled)
                                arFunctions.updateDemandStatsFromHttp(gRequest, senderAddr.getAddress());
                        }

                    }

                    LastHttpContent trailer = (LastHttpContent) msg;
                    if (!trailer.trailingHeaders().isEmpty()) {
                        buf.append("\r\n");
                        for (CharSequence name : trailer.trailingHeaders()
                                .names()) {
                            for (CharSequence value : trailer.trailingHeaders()
                                    .getAll(name)) {
                                buf.append("TRAILING HEADER: ");
                                buf.append(name).append(" = ").append(value)
                                        .append("\r\n");
                            }
                        }
                        buf.append("\r\n");
                    }
                    if (!writeResponse(trailer, ctx)) {
                        // If keep-alive is off, close the connection once the content is fully written.
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                    }
                }

            }

        }

        private void handleReceivedXDNRequest(ChannelHandlerContext ctx, Object msg) throws Exception {
            XDNRequest xdnHttpRequest = new XDNRequest();

            if (msg instanceof HttpRequest) {
                this.request = (HttpRequest) msg;
                xdnHttpRequest.setHttpRequest((HttpRequest) msg);
                if (HttpUtil.is100ContinueExpected((HttpRequest) msg)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof HttpContent) {
                xdnHttpRequest.setHttpContent((HttpContent) msg);
            }

            if (msg instanceof LastHttpContent) {

                boolean isKeepAlive = HttpUtil.isKeepAlive(xdnHttpRequest.getHttpRequest());

                // return http bad request if service name is not available
                if (!isContainServiceName(xdnHttpRequest)) {
                    sendBadRequestResponse(
                            "unspecified xdn service name",
                            ctx,
                            isKeepAlive);
                    return;
                }

                // prepare the callback for this http request
                XDNHTTPExecutedCallback callback = new XDNHTTPExecutedCallback(xdnHttpRequest, ctx);

                // create gigapaxos' request, it is important to explicitly set the clientAddress,
                // otherwise, down the pipeline, the RequestPacket's equals method will return false
                // and our callback will not be called, leaving the client hanging
                // waiting for response.
                ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(xdnHttpRequest);
                String[] addrPort = ctx.channel().remoteAddress().toString().split(":");
                gpRequest.setClientAddress(new InetSocketAddress(
                        InetAddress.getByName(addrPort[0].substring(1)),
                        Integer.parseInt(addrPort[1])));

                // forward http request to XDN App, which eventually will forward it to the service.
                // Note that response later will be written inside the callback, via ctx.
                arFunctions.handRequestToAppForHttp(gpRequest, callback);
            }
        }

        private boolean isContainServiceName(XDNRequest request) {
            String serviceName = request.getServiceName();
            return serviceName != null;
        }

        private static void sendBadRequestResponse(String message, ChannelHandlerContext ctx, boolean isKeepAlive) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, BAD_REQUEST,
                    Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));

            // Add 'Content-Length' header only for a keep-alive connection.
            // Add keep alive header as per:
            // http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            if (isKeepAlive) {
                response.headers().setInt(
                        HttpHeaderNames.CONTENT_LENGTH,
                        response.content().readableBytes());
                response.headers().set(
                        HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture cf = ctx.writeAndFlush(response);
            if (!cf.isSuccess()) {
                System.out.println("write failed: " + cf.cause());
            }

            // If keep-alive is off, close the connection once the content is fully written.
            if (!isKeepAlive) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        private static void writeHttpResponse(HttpResponse httpResponse, ChannelHandlerContext ctx,
                                              boolean isKeepAlive) {

            if (isKeepAlive) {
                httpResponse.headers().set(
                        HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture cf = ctx.writeAndFlush(httpResponse);
            cf.addListener((ChannelFutureListener) channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    System.out.println("writing response failed: " + channelFuture.cause());
                }

                // If keep-alive is off, close the connection once the content is fully written.
                if (!isKeepAlive) {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE);
                }
            });
        }

        private boolean writeResponse(HttpObject currentObj, ChannelHandlerContext ctx) {
            // Decide whether to close the connection or not.
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            // Build the response object.
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, currentObj.decoderResult().isSuccess() ? OK : BAD_REQUEST,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                // Add keep alive header as per:
                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // Encode the cookie.
            String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
            if (cookieString != null) {
                Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
                if (!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    for (Cookie cookie : cookies) {
                        response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
                    }
                }
            } else {
                // Browser sent no cookie.  Add some.
                response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode("key1", "value1"));
                response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode("key2", "value2"));
            }

            // Write the response.
            ctx.write(response);

            return keepAlive;
        }

        private static void send100Continue(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
            ctx.write(response);
        }

        private static class XDNHTTPExecutedCallback implements ExecutedCallback {
            private final ChannelHandlerContext ctx;
            private final XDNRequest request;

            public XDNHTTPExecutedCallback(XDNRequest request, ChannelHandlerContext ctx) {
                this.request = request;
                this.ctx = ctx;
            }

            @Override
            public void executed(Request executedRequest, boolean handled) {
                if (executedRequest instanceof XDNRequest xdnRequest) {
                    HttpResponse httpResponse = xdnRequest.getHttpResponse();
                    boolean isKeepAlive = HttpUtil.isKeepAlive(request.getHttpRequest());
                    if (httpResponse != null) {
                        isKeepAlive = isKeepAlive && HttpUtil.isKeepAlive(httpResponse);
                    }
                    writeHttpResponse(httpResponse, ctx, isKeepAlive);
                } else {
                    System.out.println("ERROR!!! executedRequest is not an XDNRequest anymore :(");
                }
            }
        }
    }

    /**
     * @param args
     * @throws CertificateException
     * @throws SSLException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws CertificateException, SSLException, InterruptedException {
        new HttpActiveReplica(null, new InetSocketAddress(8080), false);
    }

}
