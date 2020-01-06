package edu.umass.cs.reconfiguration.http;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLException;

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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;
import io.netty.handler.codec.http.HttpResponseStatus;
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
 * 
 * Loosely based on the HTTP Snoop server example from netty
 * documentation pages.
 * 
 * A similar implementation to {@link HttpReconfigurator}
 * 
 * 
 * 
 * @author gaozy
 *
 */
public class HttpActiveReplica {
	
	private static final Logger log = ReconfigurationConfig.getLogger();
	
	private final static int NUM_BOSS_THREADS = 4;
	
	private final static int DEFAULT_HTTP_PORT = 12416;
	private final static String DEFAULT_HTTP_ADDR = "localhost";
	
	private final static String HTTP_ADDR_ENV_KEY = "HTTPADDR";
		
	/**
	 * Whether response needs to be sent back in a synchronized manner: 
	 * true means response needs to be sent back after underlying app
	 * has successfully executed the request, i.e., the response must
	 * be sent back through {@link ExecutedCallback}; false means 
	 * response can be sent back before the underlying app executes the
	 * request.
	 */
	private final static String SYNC_KEY = "SYNC";
	
	private final EventLoopGroup bossGroup;
	private final EventLoopGroup workerGroup;
	
	private final Channel channel;
	
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
			
			log.log(Level.INFO, "HttpActiveReplica is ready on {0}", new Object[] {sockAddr});
			System.out.println( "HttpActiveReplica ready on "+sockAddr);

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
				SslContext sslCtx){
			this.arFunctions = arf;
			this.sslCtx = sslCtx;
		}		
		
		@Override
		protected void initChannel(SocketChannel channel) throws Exception {
			ChannelPipeline p = channel.pipeline();
			
			if (sslCtx != null) 
				p.addLast(sslCtx.newHandler(channel.alloc()));
			
			p.addLast(new HttpRequestDecoder());
			
			// Uncomment if you don't want to handle HttpChunks.
			p.addLast(new HttpObjectAggregator(1048576));

			p.addLast(new HttpResponseEncoder());

			p.addLast(new HttpActiveReplicaHandler(arFunctions, channel.remoteAddress()));
			
		}
		
	}
	
	private static JSONObject getJSONObjectFromHttpContent(HttpContent httpContent){
		ByteBuf content = httpContent.content();
		byte[] bytes;
		if (content.isReadable()) {
	    	bytes = new byte[content.readableBytes()];
	        content.readBytes(bytes);
	        log.log(Level.FINE, "HttpContent: {0}", new Object[]{new String(bytes)});
	        // System.out.println("Content:"+(new String(bytes)));
	    } else {
	    	return null;
	    }
			
		try {
			return new JSONObject(new String(bytes));
			
		} catch (JSONException e) {
			return null;
		}
		
	}
	
	private static void sendResponseAndCloseConnection(ChannelHandlerContext ctx) {		
		FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, BAD_REQUEST,
				Unpooled.copiedBuffer("".toString(), CharsetUtil.UTF_8));			
		ctx.writeAndFlush(httpResponse);
		ctx.close();
	}
	
	private static ChannelFuture sendResponse(ChannelHandlerContext ctx, HttpResponseStatus status) {
		FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, status,
				Unpooled.copiedBuffer("".toString(), CharsetUtil.UTF_8));			
		return ctx.writeAndFlush(httpResponse);    	
	}
	
	
	private static ChannelFuture sendResponseWithContent(ChannelHandlerContext ctx, String content, HttpResponseStatus status) {
		FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, status,
				Unpooled.copiedBuffer(content.toString(), CharsetUtil.UTF_8));			
		return ctx.writeAndFlush(httpResponse);    	
	}
	
	private static class HttpExecutedCallback implements ExecutedCallback {

		ChannelHandlerContext ctx;
		
		HttpExecutedCallback(ChannelHandlerContext ctx){
			this.ctx = ctx;
		}
		
		@Override
		public void executed(Request response, boolean handled) {
			// System.out.println("Handled: "+handled+", Response: "+response);
			if (!handled) {
				sendResponseAndCloseConnection(ctx);
			}
			ChannelFuture channelFuture = sendResponseWithContent(ctx, 
					((HttpActiveReplicaRequest) response).response != null? ((HttpActiveReplicaRequest) response).response : response.toString(), 
							OK);
			channelFuture.addListener(ChannelFutureListener.CLOSE);
		}
		
	}
	
	@SuppressWarnings("unused")
	private static Object getValueFromKey(JSONObject json, String key) throws JSONException {
		if (json.has(key)) {
			return json.get(key);
		} else if (json.has(key.toLowerCase())) {
			return json.get(key.toLowerCase());
		} 
		return null;
	}
	
	private static class HttpActiveReplicaHandler extends
		SimpleChannelInboundHandler<Object> {

		ActiveReplicaFunctions arFunctions;
		final InetSocketAddress senderAddr;
		
		HttpActiveReplicaHandler(ActiveReplicaFunctions arFunctions, InetSocketAddress addr) {
			this.arFunctions = arFunctions;
			this.senderAddr = addr;
		}
		
		@Override
		protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
			if (!(msg instanceof HttpContent) || !(msg instanceof HttpRequest)) {
				log.log(Level.FINE, "Unrecognized request: {0}", new Object[] { msg });
			}
			
			HttpRequest httpRequest = (HttpRequest) msg;
			
			HttpMethod method = httpRequest.method();
			
			HttpContent httpContent = (HttpContent) msg;
			
			if (HttpMethod.GET.equals(method)) {
				ChannelFuture channelFuture = sendResponse(ctx, OK);
				channelFuture.addListener(ChannelFutureListener.CLOSE);
			} else if (HttpMethod.POST.equals(method)) {
				
			} else {
				log.log(Level.FINE, "Unsupported operation: {0}", new Object[]{ method });
				return;
			}
			
		    HttpResponseStatus status = OK;
		    // If decoded unsuccessfully, return immediately
		    if ( !httpContent.decoderResult().isSuccess() ) {
		    	sendResponseAndCloseConnection(ctx);
		    	return;
		    }
		    
		    // Construct the request
		    JSONObject json = getJSONObjectFromHttpContent(httpContent);
		    if (json == null) {
		    	// bad json request
		    	sendResponseAndCloseConnection(ctx);
		    	return;
		    }
		    
 			/** 
 			 * Packet type should not rely on 
 			 * the underlying app for the http front end. It must be a general
 			 * purpose request so that it can go through the whole GigaPaxos
 			 * protocol stack.
 			 */
		    
		    Request request = new HttpActiveReplicaRequest(json); 
		    // System.out.println("<<<<<<<<<<<<<<<<<<<<<<< REQUEST:"+ReplicableClientRequest.wrap(request));				    
		    		
		    /*
		     * If sync is false, callback is not necessary. 
		     * Since request has been processed correctly until here, so we send back a 
		     * OK response to let the client know.
		     * Otherwise, we need to create a callback to send back a response to client.
		     */
		    boolean sync = json.has(SYNC_KEY)? json.getBoolean(SYNC_KEY): true;
		    
		    // System.out.println("SYNC key:"+ActiveReplicaHTTPKeys.SYNC.toString()+", sync:"+sync);
		    
		    ExecutedCallback callback = null;
		    if (sync){
		    	// callback to send back http response
		    	callback = new HttpExecutedCallback(ctx);
		    }
		    
		    boolean handled = false;
		    			
			// execute coordinated request here				
			if (arFunctions != null) { 
				log.log(Level.FINE, "App {0} executes request: {1}", new Object[]{ arFunctions, request });
				handled = arFunctions.handRequestToAppForHttp(
						ReplicableClientRequest.wrap(request), 
						callback);
			}							
			
			if (!handled){
				status = BAD_REQUEST;
			} else {
				arFunctions.updateDemandStatsFromHttp(request, senderAddr.getAddress() );
			}
			
			if (!sync) {
				// If not synchronized, send back response
				ChannelFuture channelFuture = sendResponse(ctx, status);
				channelFuture.addListener(ChannelFutureListener.CLOSE);
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
		new HttpActiveReplica(null, new InetSocketAddress(12416), false); 
	}
	
}
