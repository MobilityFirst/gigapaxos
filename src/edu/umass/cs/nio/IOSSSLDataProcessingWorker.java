/* Copyright (c) 2015 University of Massachusetts
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 * 
 * Initial developer(s): V. Arun */
package edu.umass.cs.nio;

import edu.umass.cs.nio.interfaces.DataProcessingWorker;
import edu.umass.cs.nio.interfaces.HandshakeCallback;
import edu.umass.cs.nio.interfaces.InterfaceMessageExtractor;
import edu.umass.cs.nio.nioutils.NIOInstrumenter;
import edu.umass.cs.utils.Util;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.security.NoSuchAlgorithmException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/*-[
	#import <Security/SecureTransport.h>
	#import <Security/Security.h>
]-*/

/**
 * @author arun
 *
 *         This class does SSL wrap/unwrap functions respectively right before
 *         NIO performs an (unencrypted) outgoing network write and right after
 *         NIO receives (encrypted) incoming network reads. It is really not a
 *         "message extractor", just an InterfaceDataProcessingWorker + wrap
 *         functionality, but it implements InterfaceMessageExtractor so that it
 *         is easy for MessageNIOTransport to conduct its functions without
 *         worrying about whether NIOTransport.worker is
 *         InterfaceMessageExtractor or not.
 * 
 *         Nevertheless, this class can be used simply with NIOTransport for
 *         secure byte stream communication as InterfaceMessageExtractor is also
 *         an InterfaceDataProcessingWorker.
 */
public class IOSSSLDataProcessingWorker implements InterfaceMessageExtractor {


	/*-[
	// SocketChannel to SSLContextRef dictionary
	NSMapTable *sslDict = nil;

// Put encrypted bytes in *data from socket
OSStatus readEncrypted(SSLConnectionRef connection, void *data, size_t *dataLength)
{
  JavaNioChannelsSocketChannel *socketChannel = (JavaNioChannelsSocketChannel*) connection;

  JavaNioByteBuffer *bbuf = JavaNioByteBuffer_allocateWithInt_((int)*dataLength);
  jint numRead = [((JavaNioChannelsSocketChannel *) nil_chk(socketChannel)) readWithJavaNioByteBuffer:bbuf];

  if (numRead == -1) {
    // Conn. closed. TODO: Call cleanup, maybe if required
    return errSSLClosedAbort;
  } else if (numRead < (int) *dataLength)
  {
    *dataLength = numRead;
    return errSSLWouldBlock;
  }

  *dataLength = numRead;
  if (numRead > 0) {
    [bbuf flip];
    IOSByteArray *encrypted = [IOSByteArray arrayWithLength:[bbuf remaining]];
    [bbuf getWithByteArray:encrypted];
    NSData *nsd = [encrypted toNSData];
    memcpy(data, nsd.bytes, [nsd length]);
  }
  return noErr;
}

// Put encrypted bytes from *data to socket
OSStatus writeEncrypted(SSLConnectionRef connection, const void *data, size_t *dataLength)
{
  //TODO
  // Copy data to encrypted byte buffer
  if (data == NULL){
    NSLog(@"NULL :(");
  }

  IOSByteArray *bytes = [IOSByteArray arrayWithLength:(unsigned int)*dataLength];
  memcpy(bytes->buffer_, data, *dataLength);

  JavaNioByteBuffer *encrypted = JavaNioByteBuffer_wrapWithByteArray_(bytes);
  JavaNioChannelsSocketChannel *socketChannel = (JavaNioChannelsSocketChannel*) connection;
  @try {
    *dataLength = 0;
    for (jint attempts = 0; (attempts < 1 && [encrypted hasRemaining]); attempts++)
    {
      int written = [((JavaNioChannelsSocketChannel *) nil_chk(socketChannel)) writeWithJavaNioByteBuffer:encrypted];
      *dataLength = *dataLength + written;
    }
  }
  @catch (JavaIoIOException *exc) {
    NSLog(@"IOException :(");
    @throw create_JavaLangIllegalStateException_initWithJavaLangThrowable_(exc);
  }
  @catch (JavaLangIllegalStateException *exc) {
    @throw create_JavaLangIllegalStateException_initWithJavaLangThrowable_(exc);
  }

  return noErr;
}
	]-*/

	// to handle incoming decrypted data
	private final DataProcessingWorker decryptedWorker;

	private final ExecutorService taskWorkers = Executors.newFixedThreadPool(4);

	private ConcurrentHashMap<SelectableChannel, NonBlockingSSLImpl> sslMap = new ConcurrentHashMap<SelectableChannel, NonBlockingSSLImpl>();

	// to signal connection handshake completion to transport
	private HandshakeCallback callbackTransport = null;

	protected final SSLDataProcessingWorker.SSL_MODES sslMode;

	private String myID = null;
	private static final Logger log = NIOTransport.getLogger();

	/**
	 * @param worker
	 * @param sslMode
	 * @throws NoSuchAlgorithmException
	 * @throws SSLException
	 */
	protected IOSSSLDataProcessingWorker(DataProcessingWorker worker,
                                         SSLDataProcessingWorker.SSL_MODES sslMode, String myID)
			throws NoSuchAlgorithmException, SSLException {
		this.decryptedWorker = worker;
		this.sslMode = sslMode;
		this.myID = myID;
	}

	protected IOSSSLDataProcessingWorker setHandshakeCallback(
			HandshakeCallback callback) {
		this.callbackTransport = callback;
		return this;
	}

	/*-[
	void processData(EduUmassCsNioIOSSSLDataProcessingWorker *self,
					JavaNioChannelsSocketChannel *channel, JavaNioByteBuffer *encrypted){
		jint length = 4096;

		char buffer[length];;
		size_t processed = 0;
		NSValue *val = [sslDict objectForKey:channel];
		SSLContextRef sslContext = [val pointerValue];
		SSLRead(sslContext, buffer, length-1, &processed);
		IOSByteArray *decrypted = [IOSByteArray arrayWithLength:(int)processed];

		if (processed > 0)
		{
			memcpy(decrypted->buffer_, buffer, processed);
			JavaNioByteBuffer *decryptedbb = JavaNioByteBuffer_wrapWithByteArray_(decrypted);
    		[((EduUmassCsNioIOSSSLDataProcessingWorker_NonBlockingSSLImpl *) nil_chk([((JavaUtilConcurrentConcurrentHashMap *) nil_chk(self->sslMap_)) getWithId:channel])) onInboundDataWithJavaNioByteBuffer:decryptedbb];
		}
	}
	]-*/

	/* Read encrypted bytes from socketChannel and call extractMessages directly */
	@Override
	public void processData(SocketChannel channel, ByteBuffer encrypted) {

	}

	/*-[
		int wrap(JavaNioChannelsSocketChannel *channel, JavaNioByteBuffer *unencrypted)
		{
			IOSByteArray *decryptedBytes = [IOSByteArray arrayWithLength:[((JavaNioByteBuffer *) nil_chk(unencrypted)) remaining]];
			[unencrypted getWithByteArray:decryptedBytes withInt:0 withInt:decryptedBytes->size_];
			NSData *nsData = [decryptedBytes toNSData];
			NSValue *val = [sslDict objectForKey:channel];
			SSLContextRef sslContext = (SSLContextRef) [val pointerValue];
			size_t processed = 0;
			SSLWrite(sslContext, (void*)nsData.bytes, [nsData length] , &processed);
			return (int) processed;
		}
	]-*/

	// invoke SSL wrap
	protected int wrap(SocketChannel channel, ByteBuffer unencrypted) {
		return 0;
	}

	protected boolean isHandshakeComplete(SocketChannel socketChannel) {
		NonBlockingSSLImpl nioSSL = this.sslMap.get(socketChannel);
		// socketChannel may be unmapped yet or under exception
		return nioSSL != null ? ((NonBlockingSSLImpl) nioSSL)
				.isHandshakeComplete() : false;
	}

	/*-[
SSLContextRef setupSSL(JavaNioChannelsSocketChannel *socketChannel)
{
  if (!sslDict)
  {
    sslDict = [NSMapTable strongToStrongObjectsMapTable];
  }

  SSLContextRef sslContext = SSLCreateContext(kCFAllocatorDefault, kSSLClientSide, kSSLStreamType);
  SSLSetIOFuncs(sslContext, readEncrypted, writeEncrypted);
  SSLSetConnection(sslContext, (SSLConnectionRef) socketChannel);

  NSData *p12Data = [NSData dataWithContentsOfFile:[[NSBundle mainBundle] pathForResource:@"key-node100" ofType:@"p12"]];

  CFStringRef password = CFSTR("qwerty");
  const void *keys[] = { kSecImportExportPassphrase };
  const void *values[] = { password };
  CFDictionaryRef optionsDictionary = CFDictionaryCreate(NULL, keys, values, 1, NULL, NULL);
  CFArrayRef p12Items;
  OSStatus result = SecPKCS12Import((__bridge CFDataRef)p12Data, optionsDictionary, &p12Items);
  CFDictionaryRef identityDict = CFArrayGetValueAtIndex(p12Items, 0);
  SecIdentityRef identityApp =(SecIdentityRef)CFDictionaryGetValue(identityDict,kSecImportItemIdentity);
  SecCertificateRef certRef;
  SecIdentityCopyCertificate(identityApp, &certRef);
  SecCertificateRef certArray[1] = { certRef };
  CFArrayRef myCerts = CFArrayCreate(NULL, (void *)certArray, 1, NULL);

  SSLSetCertificate(sslContext, myCerts );
  SSLSetSessionOption(sslContext, kSSLSessionOptionBreakOnServerAuth, YES);
  [sslDict setObject:[NSValue valueWithPointer:sslContext] forKey:socketChannel];
  return sslContext;
}
]-*/

/*-[
bool registerInternal(EduUmassCsNioIOSSSLDataProcessingWorker *self,
					  JavaNioChannelsSelectionKey *key, bool isClient)
	{
		  jboolean handshakeSuccess = false;
  		  EduUmassCsNioIOSSSLDataProcessingWorker_NonBlockingSSLImpl *nioSSL = create_EduUmassCsNioIOSSSLDataProcessingWorker_NonBlockingSSLImpl_initWithEduUmassCsNioIOSSSLDataProcessingWorker_withJavaNioChannelsSelectionKey_(self, key);
  		  [((JavaUtilConcurrentConcurrentHashMap *) nil_chk(self->sslMap_)) putWithId:[key channel] withId:nioSSL];
  		  JavaNioChannelsSocketChannel *socketChannel = (JavaNioChannelsSocketChannel *) cast_chk([key channel], [JavaNioChannelsSocketChannel class]);
  		  SSLContextRef context = setupSSL(socketChannel);

  		  OSStatus status = errSSLWouldBlock;
		  while (status == errSSLWouldBlock){
			status= SSLHandshake(context);
		  }

		  status = errSSLWouldBlock;
			while (status == errSSLWouldBlock){
			status= SSLHandshake(context);
		  }

		  //TODO: If status is abnormal, call handshake failed callback

  		  [((EduUmassCsNioAbstractNIOSSL *) nil_chk([((JavaUtilConcurrentConcurrentHashMap *) nil_chk(self->sslMap_)) getWithId:[key channel]])) onHandshakeSuccess];
          [((JavaUtilLoggingLogger *) nil_chk(EduUmassCsNioIOSSSLDataProcessingWorker_log)) logWithJavaUtilLoggingLevel:JreLoadStatic(JavaUtilLoggingLevel, FINE) withNSString:@"{0} registered {1} socket channel {2}" withNSObjectArray:[IOSObjectArray arrayWithObjects:(id[]){ self, (isClient ? @"client" : @"server"), [key channel] } count:3 type:NSObject_class_()]];
  		  return true;
  	}
	 ]-*/

	protected boolean register(SelectionKey key, boolean isClient)
			throws IOException {
		return false;
	}

	// remove entry from sslMap, no need to check containsKey
	private void cleanup(SelectionKey key) {
		NIOTransport.cleanup(key);
		this.remove(key);
	}
	
	// called by cleanup in NIOTransport
	protected void remove(SelectionKey key) {
		SocketChannel socketChannel = (SocketChannel) key.channel();
		if (socketChannel != null) {
			NonBlockingSSLImpl nioSSL = this.sslMap.remove(socketChannel);

		}
	}

	// SSL NIO implementation
	class NonBlockingSSLImpl {
		private SelectionKey key;
		private boolean handshakeComplete = false;

		NonBlockingSSLImpl(SelectionKey key) {
			this.key = key;
		}

		// inbound decrypted data is simply handed over to worker
		public void onInboundData(ByteBuffer decrypted) {
			log.log(Level.FINEST,
					"{0} received decrypted data of length {1} bytes on channel {2}",
					new Object[] { this, decrypted.remaining(), ((SocketChannel) (key.channel())) });
			IOSSSLDataProcessingWorker.this.extractMessages(key, decrypted);
		}

		public void onHandshakeFailure(Exception cause) {
			cause.printStackTrace();
			log.log(Level.WARNING,
					"{0} encountered SSL handshake failure; cleaning up channel {1}",
					new Object[] { this, key.channel() });
			// should only be invoked by selection thread
			cleanup(key);
		}

		public void onHandshakeSuccess() {
			this.setHandshakeComplete();
			log.log(Level.FINE,
					"{0} conducted successful SSL handshake for channel {1}",
					new Object[] { this, key.channel() });
		}

		public void onClosed() {
			log.log(Level.FINE, "{0} cleaning up closed SSL channel {1}",
					new Object[] { this, key.channel() });
			cleanup(key);
		}

		public void onOutboundData(ByteBuffer encrypted) {
			SocketChannel channel = ((SocketChannel) key.channel());
			try {
				log.log(Level.FINEST,
						"{0} sending encrypted data of length {1} bytes to send on channel {2}",
						new Object[] { this, encrypted.remaining(),
								channel });
				/* The assertion is true because we initialized key in the
				 * parent constructor. This method is the only reason we need
				 * the key in the parent, otherwise AbstractNIOSSL is just an
				 * SSL template and has nothing to do with NIO. */
				assert (key != null);
				int totalLength = encrypted.remaining();
				/* Try few times, but can't really wait here except in case
				 * of handshaking when we have to send everything out.
				 */
				for (int attempts = 0; (attempts < 1 || !this
						.isHandshakeComplete()) && encrypted.hasRemaining(); attempts++)
					channel.write(encrypted);

				NIOInstrumenter.incrEncrBytesSent(totalLength - encrypted.remaining());

				// not a showstopper if we don't absolutely complete the write
				if (encrypted.hasRemaining())
					log.log(Level.FINE,
							"{0} failed to bulk-write {1} bytes despite multiple attempts ({2} bytes left unsent)",
							new Object[] { this, totalLength,
									encrypted.remaining(), });

			} catch (IOException | IllegalStateException exc) {
				// need to cleanup as we are screwed
				log.severe(this + " ran into " + exc.getClass().getSimpleName()
						+ "  while writing outbound data; closing channel");
				cleanup(key);
				throw new IllegalStateException(exc);
			}
		}

		public String toString() {
			return this.getClass().getSimpleName() + getMyID();
		}

		private synchronized boolean isHandshakeComplete() {
			return this.handshakeComplete;
		}

		private synchronized void setHandshakeComplete() {
			this.handshakeComplete = true;
			callbackTransport.handshakeComplete(this.key);
		}
	}

	public String toString() {
		return this.getClass().getSimpleName() + getMyID();
	}

	/**
	 * @return My ID, primarily for logging purposes.
	 */
	public String getMyID() {
		return this.myID;
	}

	protected void setMyID(String id) {
		this.myID = id;
	}

	public void stop() {
		this.taskWorkers.shutdownNow();
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker).stop();
	}

	@Override
	public void addPacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker)
					.addPacketDemultiplexer(pd);
	}
	@Override
	public void precedePacketDemultiplexer(AbstractPacketDemultiplexer<?> pd) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker)
					.precedePacketDemultiplexer(pd);
	}

	@Override
	public void processLocalMessage(InetSocketAddress sockAddr, byte[] msg) {
		if (this.decryptedWorker instanceof InterfaceMessageExtractor)
			((InterfaceMessageExtractor) this.decryptedWorker)
					.processLocalMessage(sockAddr, msg);

	}

	private void extractMessages(SelectionKey key, ByteBuffer incoming) {
		ByteBuffer bbuf = null;
		try {
			while (incoming.hasRemaining()
					&& (bbuf = this.extractMessage(key, incoming)) != null) {
				this.decryptedWorker.processData((SocketChannel) key.channel(),
						bbuf);
			}
		} catch (IOException e) {
			log.severe(this + e.getMessage() + " on channel " + key.channel());
			e.printStackTrace();
			// incoming is emptied out; what else to do here?
		}
	}

	// extracts a single message
	private ByteBuffer extractMessage(SelectionKey key, ByteBuffer incoming)
			throws IOException {
		NIOTransport.AlternatingByteBuffer abbuf = (NIOTransport.AlternatingByteBuffer) key
				.attachment();
		assert (abbuf != null);
		if (abbuf.headerBuf.remaining() > 0) {
			Util.put(abbuf.headerBuf, incoming);
//			abbuf.readHeader(incoming);
			if (abbuf.headerBuf.remaining() == 0) {
				abbuf.bodyBuf = ByteBuffer.allocate(NIOTransport
						.getPayloadLength((ByteBuffer) abbuf.headerBuf.flip()));
				assert (abbuf.bodyBuf != null && abbuf.bodyBuf.capacity() > 0);
			}
		}
		ByteBuffer retval = null;
		if (abbuf.bodyBuf != null) {
			Util.put(abbuf.bodyBuf, incoming);
			if (abbuf.bodyBuf.remaining() == 0) {
				retval = (ByteBuffer) abbuf.bodyBuf.flip();
				abbuf.clear(); // prepare for next read
			}
		}
		return retval;
	}

	@Override
	public void demultiplexMessage(Object message) {
		this.decryptedWorker.demultiplexMessage(message);
	}
}
