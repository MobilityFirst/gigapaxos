package edu.umass.cs.reconfiguration.dns;

import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;

import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorFunctions;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.BasicReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.RequestActiveReplicas;
import edu.umass.cs.utils.Config;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioDatagramChannel;
import io.netty.handler.codec.dns.DatagramDnsQuery;
import io.netty.handler.codec.dns.DatagramDnsQueryDecoder;
import io.netty.handler.codec.dns.DatagramDnsResponse;
import io.netty.handler.codec.dns.DatagramDnsResponseEncoder;
import io.netty.handler.codec.dns.DefaultDnsQuestion;
import io.netty.handler.codec.dns.DefaultDnsRawRecord;
import io.netty.handler.codec.dns.DnsRecordType;
import io.netty.handler.codec.dns.DnsSection;

/**
 * This is the authoritative DNS resolver running in front of RCs.
 * The DnsHandler handles a DNS request by taking the following steps:
 *
 * 1. It checks the query type, if the type in the request is not A, return no answer
 * 2. If it's a type-A query, then create a RequestReplicaActives packet
 * 3. It calls ReconfiguratorFunctions.sendRequest to retrieve the active replicas
 * 4. It sends back response according to the result set
 *
 * @author gaozy
 */
public class DnsReconfigurator {

    // set default TTL to 30s
    static int defaultTTL = Config.getGlobalInt(ReconfigurationConfig.RC.DEFAULT_DNS_TTL);
    
    // the timeout for RequestActiveReplicas request
    // final static long timeout = 5; // seconds

    private static final Logger log = ReconfigurationConfig.getLogger();
    
    final private String defaultTrafficPolicy;
    
    private static DnsTrafficPolicy policy;
    
    /**
     * @param app
     */
    public DnsReconfigurator(ReconfiguratorFunctions app) {
    	defaultTrafficPolicy = Config.getGlobalString(ReconfigurationConfig.RC.DEFAULT_DNS_TRAFFIC_POLICY_CLASS);
    	
    	Class<?> c;
		try {
			c = Class.forName(defaultTrafficPolicy);
			policy = (DnsTrafficPolicy) c.getConstructor().newInstance();
			log.log(Level.INFO, "{0} creates dns traffic policy with the class name {1}", new Object[]{this, defaultTrafficPolicy});
		} catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException | SecurityException e) {
			e.printStackTrace();
			log.log(Level.WARNING, "{0} unable to create dns traffic policy with the class name {1}", new Object[]{this, defaultTrafficPolicy});
		}

    	
    	
        final NioEventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(group).channel(NioDatagramChannel.class)
                    .handler(new ChannelInitializer<NioDatagramChannel>() {
                        @Override
                        protected void initChannel(NioDatagramChannel nioDatagramChannel) throws Exception {
                            nioDatagramChannel.pipeline().addLast(new DatagramDnsQueryDecoder());
                            nioDatagramChannel.pipeline().addLast(new DatagramDnsResponseEncoder());
                            nioDatagramChannel.pipeline().addLast(new DnsHandler(app));                           
                        }
                    }).option(ChannelOption.SO_BROADCAST, true);

            // need admin privilege to bind to port 53 
            ChannelFuture future = bootstrap.bind(53).sync();           
            future.channel().closeFuture().sync();
            
            log.log(Level.FINE, "DNS server {0} bootup successfully", new Object[]{this});
            
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            group.shutdownGracefully();
        }
    }

    @ChannelHandler.Sharable
    static class DnsHandler extends SimpleChannelInboundHandler<DatagramDnsQuery> {
    	
    	private ReconfiguratorFunctions rcf;
    	
    	DnsHandler(ReconfiguratorFunctions rcf){
    		this.rcf = rcf;
    	}
    	
    	private byte[] convertIpStringToByteArray(String ip) throws UnknownHostException {
            return InetAddress.getByName(ip).getAddress();
        }
    	
    	private static final ReconfiguratorRequest toReconfiguratorRequest(
    			String name, Channel channel) throws JSONException {

    		JSONObject json = new JSONObject();
    		
    		ReconfigurationPacket.PacketType type = ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS;
    		
    		// generate a random ID
    		long requestID = (long) Math.random()*Integer.MAX_VALUE;
    					
    		// create a ClientReconfigurationPacket; insert necessary fields
    		json.put(JSONPacket.PACKET_TYPE, type.getInt())
    				.put(BasicReconfigurationPacket.Keys.NAME.toString(), name)
    				// epoch can be always set to 0
    				.put(BasicReconfigurationPacket.Keys.EPOCH.toString(), 0)
    				// is_query is always true at this server
    				.put(ClientReconfigurationPacket.Keys.IS_QUERY.toString(), true)
    				// *some* creator needed for inter-reconfigurator forwarding
    				.put(ClientReconfigurationPacket.Keys.CREATOR.toString(),
    						channel.remoteAddress())
    				// myReceiver probably not necessary
    				.put(ClientReconfigurationPacket.Keys.MY_RECEIVER.toString(),
    						channel.localAddress())
    						// request ID
    						.put(RequestActiveReplicas.Keys.QID.toString(), requestID)

    		;

    		ClientReconfigurationPacket crp = null;
    		try {
    			crp = (ClientReconfigurationPacket) ReconfigurationPacket
    					.getReconfigurationPacket(json,
    							ClientReconfigurationPacket.unstringer);
    		} catch (Exception e) {
    			e.printStackTrace();
    		}
    		return crp;
    	}
    	
        @Override
        public void channelRead0(ChannelHandlerContext ctx, DatagramDnsQuery query) throws UnsupportedEncodingException {
        	
            DatagramDnsResponse response = new DatagramDnsResponse(query.recipient(), query.sender(), query.id());
            try {
                DefaultDnsQuestion dnsQuestion = query.recordAt(DnsSection.QUESTION);
                response.addRecord(DnsSection.QUESTION, dnsQuestion);
                log.log(Level.FINE, "{0} receives a DNS query {1}", new Object[]{this, query});
                
                // remove the dot from the end of the domain name 
                String name = dnsQuestion.name();
                if (name.endsWith("."))
                	name = name.substring(0, name.length()-1);
                
                // NOTE: we only handle A type DNS query
                if(dnsQuestion.type() != DnsRecordType.A) {
                	// no answer in the response
                	ctx.writeAndFlush(response);
                    return;
                }

                
                /**
                 * send REQUEST_ACTIVE_REPLICA request:
                 * we use the querier's socket address as the initiator's socket address
                 */
                ReconfiguratorRequest request = toReconfiguratorRequest(name, ctx.channel());
                log.log(Level.FINE, "{0} sends ReconfiguratorRequest request {1} to fetch actives for name {2}", new Object[]{this, request, name});
                ReconfiguratorRequest resp = null;
                if (rcf != null){
                	resp = this.rcf.sendRequest(request);
                }
                
                log.log(Level.FINE, "{0} receives ReconfiguratorRequest response for name {1}: {2}", new Object[]{this, name, resp});
                /**
                 * resp being null means no answer found for the domain name/service name, return directly
                 */
                if (resp == null || resp.isFailed()){
                	// no answer in the response, or resp indicates the request failed
                	ctx.writeAndFlush(response);
                	return;
                }
                
                
                Set<InetAddress> result = new HashSet<>();
                for ( InetSocketAddress addr : ((RequestActiveReplicas) resp).getActives() ){
                	result.add(addr.getAddress());
                }
                 
                Set<InetAddress> r = policy.getAddresses(result, query.sender().getAddress());
                
                for(InetAddress address : r){
                	DefaultDnsRawRecord queryAnswer = new DefaultDnsRawRecord(dnsQuestion.name(), DnsRecordType.A, defaultTTL,
                            Unpooled.wrappedBuffer(convertIpStringToByteArray(address.getHostAddress())));
                    response.addRecord(DnsSection.ANSWER, queryAnswer);
                }
                ctx.writeAndFlush(response);
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            cause.printStackTrace();
        }
    }
    
    @Override
    public String toString(){
    	return this.getClass().getSimpleName();
    }
    
    /**
     * @param args
     */
    public static void main(String[] args){
    	new DnsReconfigurator(null);
    }
}
