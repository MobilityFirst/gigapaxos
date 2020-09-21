package edu.umass.cs.gigapaxos.paxosutil;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosConfig.PC;
import edu.umass.cs.gigapaxos.SQLPaxosLogger;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.StringLocker;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         A utility class for helping applications create and restore from
 *         large checkpoints.
 */
public class LargeCheckpointer {

	static {
		PaxosConfig.load();
	}

	/**
	 * The random number suffixes below are in order to make it unlikely that an
	 * application checkpoint is JSON-formatted exactly as a checkpoint handle.
	 */
	public static enum Keys {
		/**
		 * 
		 */
		ISA3142,

		/**
		 * 
		 */
		FNAME2178,

		/**
		 * 
		 */
		FSIZE6022
	};

	private static final String CHECKPOINTS_DIR = "paxos_large_checkpoints";

	private static final StringLocker stringLocker = new StringLocker();

	private final String checkpointDir;
	private final String myID;
	private ServerSocket serverSock;
	private ScheduledExecutorService executor;
	private boolean closed = false;

	private static Logger log = PaxosConfig.getLogger();

	/**
	 * @param dir
	 * 
	 * @param myID
	 *            A unique ID for the node using this LargeCheckpointer
	 *            instance.
	 */
	public LargeCheckpointer(String dir, String myID) {
		this.checkpointDir = (dir = (dir == null ? Config
				.getGlobalString(PC.GIGAPAXOS_DATA_DIR) + "/" + PC.PAXOS_LOGS_DIR.getDefaultValue() : dir))
				+ (dir.endsWith("/") ? "" : "/");
		this.myID = myID;
		initCheckpointServer();
	}

	/**
	 * @param myID
	 */
	public LargeCheckpointer(String myID) {
		this(null, myID);
	}

	private static final String CHARSET = "ISO-8859-1";

	/**
	 * {@link Replicable} applications can use this method to fetch the
	 * checkpoint represented by the handle to a local file
	 * {@code localFilename}. This method is useful in
	 * {@link Replicable#restore(String, String)}.
	 * 
	 * The parent directory of the file {@code localFilename} must exist,
	 * otherwise {@link NoSuchFileException} will be thrown.
	 * 
	 * Applications are expected to delete {@code localFilename} after they are
	 * done restoring from it.
	 * 
	 * @param handle
	 * @param localFilename
	 * @return The file holding the checkpoint.
	 */
	public static String restoreCheckpointHandle(String handle,
			String localFilename) {
		if (!isCheckpointHandle(handle))
			throw new RuntimeException(
					"Supplied handle is not a correctly formatted paxos checkpoint handle: "
							+ handle);
		return LargeCheckpointer.fetchCheckpoint(handle, localFilename);
	}

	/**
	 * {@link Replicable} applications can use this method to create a
	 * checkpoint handle after they have created a checkpoint in the file
	 * {@code filename}.
	 * 
	 * @param filename
	 * @return {@code filname} formatted as a paxos checkpoint handle.
	 */
	public static String createCheckpointHandle(String filename) {
		JSONObject json = new JSONObject();
		try {
			json.put(Keys.ISA3142.toString(), JSONObject.NULL);
			json.put(Keys.FNAME2178.toString(), filename);
			json.put(Keys.FSIZE6022.toString(), new File(filename).length());
		} catch (JSONException e) {
			e.printStackTrace();
			return null;
		}
		return json.toString();
	}

	/**
	 * @param string
	 * @return True if string is a correctly formatted checkpoint handle.
	 */
	public static boolean isCheckpointHandle(String string) {
		if (string == null)
			return false;
		try {
			JSONObject json = new JSONObject(string);
			return json.has(Keys.ISA3142.toString())
					&& json.has(Keys.FNAME2178.toString())
					&& json.has(Keys.FSIZE6022.toString())
					;
		} catch (JSONException e) {
			return false;
		}
	}

	/**
	 * Sends request for and receives remote checkpoint file if correctly
	 * formatted URL. If so, it returns a local filename. If not, it returns the
	 * url back as-is.
	 * 
	 * @param url
	 * @param localFilename
	 * @return Local filename or url depending on outcome.
	 */
	private static String fetchCheckpoint(String url, String localFilename) {
		if (url == null)
			return url;
		String filename = url;
		JSONObject jsonUrl;
		try {
			jsonUrl = new JSONObject(url);
			if (jsonUrl.has(Keys.ISA3142.toString())
					&& jsonUrl.has(Keys.FNAME2178.toString())
					&& jsonUrl.has(Keys.FSIZE6022.toString())) {
				filename = jsonUrl.getString(Keys.FNAME2178.toString());
				File file = new File(filename);
				assert (jsonUrl.get(Keys.ISA3142.toString()) != null);

				/* If file exists, it must have been created locally or fetched
				 * previously from a remote node. */
				if (!file.exists())
					// fetch from remote (possibly localhost)
					filename = fetchRemoteCheckpoint(
							Util.getInetSocketAddressFromString(jsonUrl
									.getString(Keys.ISA3142.toString())),
							filename,
							jsonUrl.getLong(Keys.FSIZE6022.toString()),
							// save with same filename first
							filename);
				// then copy to requested filename
				if (!filename.equals(localFilename))
					Files.copy(Paths.get(filename), Paths.get(localFilename),
							StandardCopyOption.REPLACE_EXISTING);
			}
		} catch (JSONException e) {
			// do nothing, will return filename
		} catch (IOException e) {
			e.printStackTrace();
		}
		return localFilename;
	}

	private boolean deleteOldCheckpoints(final String cpDir,
			final String rcGroupName, int keep) {
		return LargeCheckpointer.deleteOldCheckpoints(cpDir, rcGroupName, keep,
				this);
	}

	/* Deletes all but the most recent checkpoint for the RC group name. We
	 * could track recency based on timestamps using either the timestamp in the
	 * filename or the OS file creation time. Here, we just supply the latest
	 * checkpoint filename explicitly as we know it when this method is called
	 * anyway. */
	private static boolean deleteOldCheckpoints(final String cpDir,
			final String paxosID, int keep, Object lockMe) {
		File dir = new File(cpDir);
		assert (dir.exists());
		// get files matching the prefix for this rcGroupName's checkpoints
		File[] foundFiles = dir.listFiles(new FilenameFilter() {
			public boolean accept(File dir, String name) {
				return name.startsWith(paxosID);
			}
		});
		// delete all but the most recent
		boolean allDeleted = true;
		for (Filename f : getAllButLatest(foundFiles, keep))
			allDeleted = allDeleted && deleteFile(f.file, lockMe);
		return allDeleted;
	}

	private static final long MAX_FINAL_STATE_AGE = Config
			.getGlobalLong(PC.MAX_FINAL_STATE_AGE);

	private static boolean deleteFile(File f, Object lockMe) {
		long age = 0;
		if ((age = System.currentTimeMillis() - Filename.getLTS(f)) > MAX_FINAL_STATE_AGE) {
			log.log(Level.INFO,
					"{0} deleting old checkpoint file {1} created {2} (> {3}={4}) seconds back",
					new Object[] { LargeCheckpointer.class.getSimpleName(),
							f.toPath(), age / 1000,
							PC.MAX_FINAL_STATE_AGE.toString(),
							MAX_FINAL_STATE_AGE / 1000 });
			synchronized (lockMe) {
				return f.delete();
			}
		}
		return true;
	}

	private static Set<Filename> getAllButLatest(File[] files, int keep) {
		TreeSet<Filename> allFiles = new TreeSet<Filename>();
		TreeSet<Filename> oldFiles = new TreeSet<Filename>();
		for (File file : files)
			allFiles.add(new Filename(file));
		if (allFiles.size() <= keep)
			return oldFiles;
		Iterator<Filename> iter = allFiles.iterator();
		for (int i = 0; i < allFiles.size() - keep; i++)
			oldFiles.add(iter.next());

		return oldFiles;
	}

	private static class Filename implements Comparable<Filename> {
		final File file;

		Filename(File f) {
			this.file = f;
		}

		@Override
		public int compareTo(LargeCheckpointer.Filename o) {
			long t1 = getLTS(file);
			long t2 = getLTS(o.file);

			if (t1 < t2)
				return -1;
			else if (t1 == t2)
				return 0;
			else
				return 1;
		}

		private static long getLTS(File file) {
			String[] tokens = file.toString().split("\\.");
			assert (tokens[tokens.length - 1].matches("[0-9a-fA-F]*$")) : file;
			try {
				return SQLPaxosLogger.USE_HEX_TIMESTAMP ? Long.parseLong(
						tokens[tokens.length - 1], 16) : Long
						.valueOf(tokens[tokens.length - 1]);
			} catch (NumberFormatException nfe) {
				nfe.printStackTrace();
			}
			return file.lastModified();
		}
	}

	/**
	 * Helper function for getRemoteCheckpoint above that actually fetches the
	 * reads from the socket and writes to a local file.
	 * 
	 * @param paxosID
	 * @param sockAddr
	 * @param remoteFilename
	 * @param fileSize
	 * @return
	 */
	private static String fetchRemoteCheckpoint(InetSocketAddress sockAddr,
			String remoteFilename, long fileSize, String localFilename) {
		log.log(Level.FINE, "LargeCheckpointer.fetchRemoteCheckpoint: about to fetch from {0} to get {1} and put at {2}", 
				new Object[]{sockAddr, remoteFilename, localFilename});
		synchronized (stringLocker.get(localFilename)) {
			String request = remoteFilename + "\n";
			Socket sock = null;
			FileOutputStream fos = null;
			try {
				sock = new Socket(sockAddr.getAddress(), sockAddr.getPort());
				log.log(Level.FINE, "LargeCheckpointer.fetchRemoteCheckpoint: connected {0} to get {1} and put at {2}",
						new Object[]{sockAddr, remoteFilename, localFilename});
				sock.getOutputStream().write(request.getBytes(CHARSET));
				InputStream inStream = (sock.getInputStream());
				if (!createCheckpointFile(localFilename)){
					log.warning("LargeCheckpointer.fetchRemoteCheckpoint: failed to create "+localFilename);
					return null;
				}
				log.log(Level.FINE, "LargeCheckpointer.fetchRemoteCheckpoint: file created successfully {0}", new Object[]{localFilename});
				fos = new FileOutputStream(new File(localFilename));
				byte[] buf = new byte[1024];
				int nread = 0;
				int nTotalRead = 0;
				// read from sock, write to file
				while ((nread = inStream.read(buf)) >= 0) {
					/* FIXME: need to ensure that the read won't block forever
					 * if the remote endpoint crashes ungracefully and there is
					 * no exception triggered here. */
					nTotalRead += nread;
					fos.write(buf, 0, nread);
				}
				fos.flush();
				// check exact expected file size
				if (nTotalRead != fileSize) {
					log.log(Level.WARNING, "LargeCheckpointer.fetchRemoteCheckpoint: expect {0} bytes, but read {1} bytes, delete file {2}",
							new Object[]{fileSize, nTotalRead, localFilename});
					new File(localFilename).delete();
					localFilename = null;
				}
			} catch (IOException e) {
				e.printStackTrace();
				localFilename = null;
			} finally {
				try {
					if (fos != null)
						fos.close();
					if (sock != null)
						sock.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return localFilename;
		}
	}

	private String getCheckpointDir() {
		return this.checkpointDir + CHECKPOINTS_DIR + "/" + myID + "/";
	}

	private String getCheckpointFile(String paxosID) {
		return this.getCheckpointDir() + paxosID + "."
				+ System.currentTimeMillis();
	}

	private static boolean createCheckpointFile(String filename) {
		//synchronized (stringLocker.get(filename)) {
			File file = new File(filename);
			try {
				if (file.getParentFile().mkdirs() || file.getParentFile().exists()){
					file.createNewFile(); // will create only if the file does not exists
				}
			} catch (IOException e) {
				log.severe("Unable to create checkpoint file for " + filename);
				e.printStackTrace();
			}
			return (file.exists());
		//}
	}

	public String toString() {
		return this.getClass().getSimpleName() + myID;
	}

	/**
	 * @param name
	 * @param handle
	 * @return Fixed handle as string.
	 * @throws JSONException
	 * @throws IOException
	 */
	private String stowAwayCheckpoint(String name, String handle)
			throws JSONException, IOException {
		JSONObject json = new JSONObject(handle);
		String newFilename = null;
		if (!this.moveCheckpoint(json.getString(Keys.FNAME2178.toString()),
				newFilename = this.getCheckpointFile(name)))
			throw new IOException("Unable to move file "
					+ json.getString(Keys.FNAME2178.toString()) + " to "
					+ newFilename);

		this.deleteOldCheckpoints(getCheckpointDir(), name, 4);

		json.put(Keys.ISA3142.toString(),
				myID != null && PaxosConfig.getActives().get(myID) != null?
				new InetSocketAddress(PaxosConfig.getActives().get(myID).getAddress().getHostAddress(), this.serverSock.getLocalPort()):
				this.serverSock.getLocalSocketAddress());
		json.put(Keys.FNAME2178.toString(), newFilename);
		return json.toString();
	}

	private final boolean moveCheckpoint(String filename1, String filename2) {
		return moveFile(new File(filename1),
				new File(filename2).getAbsoluteFile());
	}

	private static final boolean moveFile(File f1, File f2) {
		f2.getParentFile().mkdirs();
		boolean moved = (f1.renameTo(f2.getAbsoluteFile()));
		return moved;
	}

	// /////// Start of file system checkpoint methods and classes /////////

	private static final int THREAD_POOL_SIZE = 4;

	// opens the server thread for file system based checkpoints
	private boolean initCheckpointServer() {
		this.executor = Executors.newScheduledThreadPool(THREAD_POOL_SIZE,
				new ThreadFactory() {
					@Override
					public Thread newThread(Runnable r) {
						Thread thread = Executors.defaultThreadFactory()
								.newThread(r);
						thread.setName(LargeCheckpointer.class.getSimpleName()
								+ ":" + myID);
						return thread;
					}
				});

		try {
			this.serverSock = new ServerSocket();
			this.serverSock.bind(new InetSocketAddress(0));
			executor.submit(new CheckpointServer());
			return true;
		} catch (IOException e) {
			log.severe(this
					+ " unable to open server socket for large checkpoint transfers");
			e.printStackTrace();
		}
		return false;
	}

	// spawns off a new thread to process file system based checkpoint request
	private class CheckpointServer implements Runnable {

		@Override
		public void run() {
			Socket sock = null;
			try {
				while ((sock = LargeCheckpointer.this.serverSock.accept()) != null) {
					executor.submit(new CheckpointTransporter(sock));
					// (new Thread(new CheckpointTransporter(sock))).start();
				}
			} catch (IOException e) {
				if (!isClosed()) {
					log.severe(myID
							+ " incurred IOException while processing checkpoint transfer request");
					e.printStackTrace();
				}
			}
		}
	}

	private boolean isClosed() {
		return this.closed;
	}

	/**
	 * 
	 */
	public void close() {
		this.closed = true;
		try {
			this.serverSock.close();
		} catch (IOException e) {
			log.severe(this + " unable to close server socket");
			e.printStackTrace();
		}
		this.executor.shutdownNow();
	}

	// use with care
	private void deleteAllCheckpointsAndClose() {
		Util.recursiveRemove(new File(this.checkpointDir + CHECKPOINTS_DIR));
		this.close();
	}

	// sends a requested file system based checkpoint
	private class CheckpointTransporter implements Runnable {

		final Socket sock;

		CheckpointTransporter(Socket sock) {
			this.sock = sock;
		}

		@Override
		public void run() {
			transferCheckpoint(sock);
		}
	}

	private static boolean deleteHandleFile(String handle) throws JSONException {
		String filename = new JSONObject(handle).getString(Keys.FNAME2178
				.toString());
		return new File(filename).delete();
	}

	// reads request and transfers requested checkpoint.
	private static void transferCheckpoint(Socket sock) {
		{
			BufferedReader brSock = null, brFile = null;
			try {
				brSock = new BufferedReader(new InputStreamReader(
						sock.getInputStream()));
				// first and only line is request
				String request = brSock.readLine();

				// synchronized to prevent concurrent file delete
				synchronized (stringLocker.get(request)) {
					if ((new File(request).exists())) {
						// request is filename
						brFile = new BufferedReader(new InputStreamReader(
								new FileInputStream(request)));
						final BufferedInputStream inStream = new BufferedInputStream(new FileInputStream(request));
						// file successfully open if here
						OutputStream outStream = sock.getOutputStream();
//						String line = null; // each line is a record
//						while ((line = brFile.readLine()) != null)
//							outStream.write(line.getBytes(CHARSET));
						
						// read in the binary file and send out
						final byte[] buffer = new byte[4096];
					    for (int read = inStream.read(buffer); read >= 0; read = inStream.read(buffer))
					        outStream.write(buffer, 0, read);
					    inStream.close();
						outStream.close();
					}
				}
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (brSock != null)
						brSock.close();
					if (brFile != null)
						brFile.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	static class TestReplicable implements Replicable {

		Map<String, String> states = new HashMap<String, String>();

		@Override
		public boolean execute(Request request) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public Request getRequest(String stringified)
				throws RequestParseException {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public Set<IntegerPacketType> getRequestTypes() {
			// TODO Auto-generated method stub
			return null;
		}

		@Override
		public boolean execute(Request request, boolean doNotReplyToClient) {
			// TODO Auto-generated method stub
			return false;
		}

		@Override
		public String checkpoint(String name) {
			if (!this.states.containsKey(name))
				return null;
			File file = new File("file"
					+ (int) (Math.random() * Integer.MAX_VALUE));
			FileOutputStream fos = null;
			try {
				fos = new FileOutputStream(file.getPath());
				fos.write(this.states.get(name).getBytes());
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return LargeCheckpointer.createCheckpointHandle(file
					.getAbsolutePath());
		}

		@Override
		public boolean restore(String name, String state) {
			if (state == null) {
				this.states.remove(name);
				return true;
			}

			String localFilename = "file"
					+ (int) (Math.random() * Integer.MAX_VALUE);
			String filename = LargeCheckpointer.restoreCheckpointHandle(state,
					localFilename);
			FileInputStream fis = null;
			try {
				fis = new FileInputStream(filename);
				int length = (int) new File(filename).length();
				byte[] buf = new byte[length]; // only for testing
				int offset = 0;
				while (offset < length)
					offset += fis.read(buf, offset, length - offset);
				String actualState = new String(buf);
				this.states.put(name, actualState);
				// delete after use
				new File(localFilename).delete();
				return true;
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (fis != null)
						fis.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			return false;
		}

		String setRandomState(String name) {
			String state = ("state:" + (long) (Math.random() * Long.MAX_VALUE));
			this.states.put(name, state);
			return state;
		}
	}

	/**
	 */
	public static class LargeCheckpointerTest extends DefaultTest {

		private static final String NAME = "name";

		/**
		 * @throws JSONException
		 * @throws IOException
		 * 
		 */
		@Test
		public void test_checkpoint() throws JSONException, IOException {
			TestReplicable app = new TestReplicable();
			String name = NAME;
			String state = app.setRandomState(name);
			String handle = (app.checkpoint(name));
			String filename = new JSONObject(handle).getString(Keys.FNAME2178
					.toString());
			String content = new String(Files.readAllBytes(Paths.get(filename)));
			assert (state.equals(content));
			new File(filename).delete();
		}

		/**
		 * @throws JSONException
		 * @throws IOException
		 */
		@Test
		public void test_restore() throws JSONException, IOException {
			TestReplicable app1 = new TestReplicable();
			TestReplicable app2 = new TestReplicable();
			LargeCheckpointer lcp1 = new LargeCheckpointer(".", "123");

			String name = NAME;
			String state = app1.setRandomState(name);
			String handle = (app1.checkpoint(name));
			handle = lcp1.stowAwayCheckpoint(name, handle);

			app2.restore(name, handle);
			assert (app2.states.get(name).equals(state));

			deleteHandleFile(handle);
			lcp1.deleteAllCheckpointsAndClose();
		}

		/**
		 * @throws JSONException
		 * @throws IOException
		 */
		@Test
		public void test_remoteCheckpointTransfer() throws JSONException,
				IOException {
			TestReplicable app1 = new TestReplicable();
			TestReplicable app2 = new TestReplicable();
			LargeCheckpointer lcp1 = new LargeCheckpointer(".", "123");

			String name = NAME;
			String state = app1.setRandomState(name);
			String handle = (app1.checkpoint(name));

			handle = lcp1.stowAwayCheckpoint(name, handle);

			app2.restore(name, handle);

			assert (app2.states.get(name).equals(state));

			deleteHandleFile(handle);
			// lcp1.deleteAllCheckpointsAndClose();
			lcp1.close();
		}
	};

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(LargeCheckpointerTest.class);
		for (Failure failure : result.getFailures()) {
			System.out.println(failure.toString());
			failure.getException().printStackTrace();
		}
	}

	/**
	 * @param pi
	 * @param lcp
	 * @return Wrapped {@link Replicable} application that stows away checkpoint
	 *         created by the application at a system location.
	 */
	public static Replicable wrap(final Replicable pi, LargeCheckpointer lcp) {
		return new Replicable() {

			@Override
			public boolean execute(Request request) {
				return pi.execute(request);
			}

			@Override
			public Request getRequest(String stringified)
					throws RequestParseException {
				return pi.getRequest(stringified);
			}

			@Override
			public Set<IntegerPacketType> getRequestTypes() {
				return pi.getRequestTypes();
			}

			@Override
			public boolean execute(Request request, boolean doNotReplyToClient) {
				return pi.execute(request, doNotReplyToClient);
			}

			@Override
			public String checkpoint(String name) {
				String checkpoint = pi.checkpoint(name);
				try {
					if (isCheckpointHandle(checkpoint))
						checkpoint = lcp.stowAwayCheckpoint(name, checkpoint);
				} catch (JSONException | IOException e) {
					e.printStackTrace();
				}
				return checkpoint;
			}

			@Override
			public boolean restore(String name, String state) {
				return pi.restore(name, state);
			}

			public String toString() {
				return pi.toString();
			}

		};
	}
}
