package edu.umass.cs.utils;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

/**
 * @author arun
 *
 *         A utility class to generate and maintain symmetric session keys. It
 *         allows a client or a server to maintain one or more session secret
 *         keys corresponding to a public key so as to use symmetric key
 *         encryption instead of public key encryption.
 * 
 *         <p>
 * 
 *         An important assumption is that rhe two endpoints already have an SSL
 *         connection, so a man-in-the-middle can not decipher any data
 *         exchanged. If this is not true, this utility class is not useful.
 * 
 *         <p>
 * 
 *         A secret key can only be proposed by the possessor of the private key
 *         corresponding to the public key that is being substituted.
 * 
 *         <p>
 * 
 *         Multiple secret keys corresponding to the same public key can get
 *         generated over time, e.g., different (multi-homed) clients all using
 *         the same public key may simultaneously maintain sessions with the
 *         same server. We will refer to the owner of the public/private key
 *         pair the "client". Generally, a client will propose a secret key by
 *         issuing a self-signed certificate. A server may also at any time
 *         respond with a certificate for the same key-pair that it may have
 *         obtained from elsewhere, e.g., from another avatar of the client. If
 *         a server or client receives a message signed by a secret key it
 *         doesn't have, it can ask the other end for the corresponding
 *         certificate.
 * 
 *         All methods and state here is static as the symmetric keys correspond
 *         only to public keys, so they can be reused even within "virtual"
 *         nodes within a JVM.
 */
public class SessionKeys {

	/**
	 * Events associated with secret session keys.
	 */
	public static enum Events {
		/**
		 * If an endpoint does not have any secret key to decrypt a secret key
		 * encrypted message it received.
		 */
		NO_SECRET_KEY,

		/**
		 * If an endpoint receives a message encrypted with a secret key that
		 * has expired.
		 */
		EXPIRED_SECRET_KEY,

		/**
		 * If an endpoint wants the other endpoint to use an alternate secret
		 * key, say because it already has reached the maximum number of secret
		 * keys it can hold for the corresponding public key or the secret key
		 * used in the received message has expired but it has other secret keys
		 * stored for the public key, it can ask the remote endpoint to use one
		 * of the existing secret keys by providing the corresponding
		 * certificate.
		 */
		USE_THIS_SKCERT,
	}

	private static final String skEncryption = "DESede";

	/**
	 * Implementations on some devices require us to completely specify the
	 * transformation in the form of "ALGORITHM/MODE/PADDING" since the mode
	 * and padding defaults are not necessarily same across providers.
	 */
	protected static final String pkPair = "RSA/ECB/PKCS1Padding";

	protected static final String keyPairAlgorithm = "RSA";

	/**
	 * A secret key certificate consists of a [secretKey, timestamp] 2-tuple
	 * signed by the corresponding public key, and all three of these together
	 * form the certificate.
	 */
	public static class SecretKeyCertificate {
		// creation timestamp for secretKey
		final long timestamp;
		// secretKey signed by private key whose signatures we seek to replace
		final byte[] encryptedSecretKey;
		// the public key corresponding to the private key certifying secretKey
		final PublicKey publicKey;

		SecretKeyCertificate(byte[] signedSecretKey, long timestamp,
				PublicKey publicKey) {
			this.timestamp = timestamp;
			this.encryptedSecretKey = signedSecretKey;
			this.publicKey = publicKey;
		}

		/**
		 * @param includePublicKey
		 * @return Secret key certificate encoded to bytes.
		 */
		public byte[] getEncoded(boolean includePublicKey) {
			byte[] encodedPublicKey = this.publicKey.getEncoded();
			ByteBuffer bbuf = ByteBuffer
					.wrap(new byte[
					// timestamp
					Long.BYTES
					// encryptedScretKey
							+ Short.BYTES + this.encryptedSecretKey.length
							// publicKey
							+ (includePublicKey ? (encodedPublicKey = this.publicKey
									.getEncoded()).length + Short.BYTES
									: 0)]).putLong(this.timestamp)
					.putShort((short) this.encryptedSecretKey.length)
					.put(this.encryptedSecretKey);
			return (includePublicKey ? bbuf.putShort(
					(short) encodedPublicKey.length).put(encodedPublicKey)
					: bbuf).array();
		}

		public String toString() {
			return "[" + this.encryptedSecretKey + "," + this.timestamp + ","
					+ this.publicKey + "]";
		}
	}

	/**
	 * @param encodedSecretKeyCertificate
	 * @return Secret key decoded from {@code encodedSecretKeyCertificate}.
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 */
	public static SecretKey getSecretKeyFromCertificate(
			byte[] encodedSecretKeyCertificate) throws InvalidKeySpecException,
			NoSuchAlgorithmException, InvalidKeyException,
			NoSuchPaddingException, IllegalBlockSizeException,
			BadPaddingException {
		return getSecretKeyFromCertificate(encodedSecretKeyCertificate, null);
	}

	/**
	 * @param encodedSecretKeyCertificate
	 * @param publicKey
	 * @return Secret key decoded from {@code encodedSecretKeyCertificate}.
	 * @throws InvalidKeySpecException
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public static SecretKey getSecretKeyFromCertificate(
			byte[] encodedSecretKeyCertificate, PublicKey publicKey)
			throws InvalidKeySpecException, NoSuchAlgorithmException,
			NoSuchPaddingException, InvalidKeyException,
			IllegalBlockSizeException, BadPaddingException {
		ByteBuffer bbuf = ByteBuffer.wrap(encodedSecretKeyCertificate);
		long timestamp = bbuf.getLong();
		// first look for previously decoded and cached secret key certificates
		if (publicKey != null) {
			Map<Long, SecretKeyAndCertificate> skeys = pk2sks.get(publicKey);
			if (skeys != null) {
				SecretKeyAndCertificate skc = skeys.get(timestamp);
				if (skc != null) {
					return skc.secretKey;
				}
			}
		}
		// else slow path using public key decryption
		int encryptedSecretKeyLength = bbuf.getShort();
		assert (encryptedSecretKeyLength > 0) : encryptedSecretKeyLength;
		byte[] encryptedSecretKey = new byte[encryptedSecretKeyLength];
		bbuf.get(encryptedSecretKey);

		if (publicKey == null)
			if (bbuf.hasRemaining()) {
				byte[] encodedPublicKey = new byte[bbuf.getShort()];
				bbuf.get(encodedPublicKey);
				publicKey = KeyFactory.getInstance(keyPairAlgorithm).generatePublic(
						new X509EncodedKeySpec(encodedPublicKey));
			} else
				throw new RuntimeException(
						"Unable to find public key to decrypt encoded secret key certificate");
		assert (publicKey != null);
		// decrypt secret key
		Cipher cipher = Cipher.getInstance(pkPair);
		cipher.init(Cipher.DECRYPT_MODE, publicKey);
		byte[] decryptedSecretKey = cipher.doFinal(encryptedSecretKey);

		SecretKey secretKey = new SecretKeySpec(decryptedSecretKey, 0,
				decryptedSecretKey.length - Long.BYTES, skEncryption);
		// cache so as to not repeat work
		SessionKeys.putSecretKey(secretKey, new SecretKeyCertificate(
				encryptedSecretKey, timestamp, publicKey));
		return secretKey;
	}

	/**
	 * @param encoded
	 * @return Encrypted secret key timestamp.
	 */
	public static long getEncryptedSecretKeyTimestamp(byte[] encoded) {
		return ByteBuffer.wrap(encoded).getLong();
	}

	protected static class SecretKeyAndCertificate {
		private final SecretKey secretKey;
		private final SecretKeyCertificate skCert;

		SecretKeyAndCertificate(SecretKey secretKey, SecretKeyCertificate skCert) {
			this.secretKey = secretKey;
			this.skCert = skCert;
		}
	}

	/**
	 * @param n
	 */
	public static void setMaxPublicKeys(int n) {
		pk2sks.setGCThresholdSize(n);
	}

	private static int maxSecretKeys = 8;

	/**
	 * Maximum number of secret keys per public key. Clients should set this to
	 * 1 as they should have at most secret key per public key at any time.
	 * 
	 * @param n
	 */
	public static void setMaxSecretKeys(int n) {
		maxSecretKeys = n;
	}

	/**
	 * Sets maximum number of secret keys per public key to 1.
	 */
	public static void setClient() {
		maxSecretKeys = 1;
	}

	private static long secretKeyLifetime = 30 * 1000;

	/**
	 * @param lifetime
	 */
	public static void setSecretKeyLifetime(long lifetime) {
		secretKeyLifetime = lifetime;
		pk2sks.setGCTimeout(lifetime);
	}

	private static final GCConcurrentHashMap<PublicKey, Map<Long, SecretKeyAndCertificate>> pk2sks = new GCConcurrentHashMap<PublicKey, Map<Long, SecretKeyAndCertificate>>(
			new GCConcurrentHashMapCallback() {
				@Override
				public void callbackGC(Object key, Object value) {
					// silent death
				}

			}, secretKeyLifetime * 2);

	/**
	 * @param publicKey
	 * @param privateKey
	 * @return Generated secret key
	 * @throws NoSuchAlgorithmException
	 */
	private static SecretKey generateSecretKey()
			throws NoSuchAlgorithmException {
		return KeyGenerator.getInstance(skEncryption).generateKey();
	}

	/**
	 * @param secretKey
	 * @param publicKey
	 * @param privateKey
	 * @return Secret key certificate generated for the public/private keypair.
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws NoSuchPaddingException
	 */
	private static SecretKeyCertificate generateSecretKeyCertificate(
			SecretKey secretKey, PublicKey publicKey, PrivateKey privateKey)
			throws NoSuchAlgorithmException, InvalidKeyException,
			UnsupportedEncodingException, IllegalBlockSizeException,
			BadPaddingException, NoSuchPaddingException {
		long timestamp = System.currentTimeMillis();
		// encrypt using private key
		Cipher cipher = Cipher.getInstance(pkPair);
		cipher.init(Cipher.ENCRYPT_MODE, privateKey);
		byte[] clear = secretKey.getEncoded();
		ByteBuffer bbuf = ByteBuffer.wrap(new byte[clear.length + Long.BYTES]);
		bbuf.put(clear);
		bbuf.putLong(timestamp);
		byte[] sign = cipher.doFinal(bbuf.array());
		return new SecretKeyCertificate(sign, timestamp, publicKey);
	}

	/**
	 * @param publicKey
	 * @param privateKey
	 * @return Generated secret key and certificate
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchPaddingException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	private static SecretKeyAndCertificate generateSecretKeyAndCertificate(
			PublicKey publicKey, PrivateKey privateKey)
			throws NoSuchAlgorithmException, InvalidKeyException,
			SignatureException, UnsupportedEncodingException,
			IllegalBlockSizeException, BadPaddingException,
			NoSuchPaddingException {
		SecretKey secretKey = generateSecretKey();
		SecretKeyCertificate skCert = generateSecretKeyCertificate(secretKey,
				publicKey, privateKey);
		return new SecretKeyAndCertificate(secretKey, skCert);
	}

	/**
	 * @param publicKey
	 * @param secretKeyID
	 * @return Stored secret key if any, else null.
	 */
	public synchronized static SecretKey getSecretKey(PublicKey publicKey,
			Long secretKeyID) {
		if (!pk2sks.containsKey(publicKey))
			return null;
		SecretKeyAndCertificate skc = pk2sks.get(publicKey).get(secretKeyID);
		return skc != null ? skc.secretKey : null;

	}

	/**
	 * @param publicKey
	 * @return SecretKey corresponding to publicKey.
	 */
	public synchronized static SecretKey getSecretKey(PublicKey publicKey) {
		if (!pk2sks.containsKey(publicKey))
			return null;
		SecretKeyAndCertificate skc = pk2sks.get(publicKey).values().iterator()
				.next();
		return skc != null ? skc.secretKey : null;
	}

	/* Returns the most recent secret key certificate for the public key. */
	private static SecretKeyAndCertificate getMostRecent(PublicKey publicKey) {
		if (!pk2sks.containsKey(publicKey))
			return null;
		SecretKeyAndCertificate mostRecent = null;
		for (SecretKeyAndCertificate skc : pk2sks.get(publicKey).values())
			if (mostRecent == null
					|| skc.skCert.timestamp > mostRecent.skCert.timestamp)
				mostRecent = skc;
		return mostRecent;
	}

	/**
	 * @param publicKey
	 * @param privateKey
	 * @return Secret key, possibly generated on demand.
	 * @throws InvalidKeyException
	 * @throws NoSuchAlgorithmException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws NoSuchPaddingException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public synchronized static SecretKey getOrGenerateSecretKey(
			PublicKey publicKey, PrivateKey privateKey)
			throws InvalidKeyException, NoSuchAlgorithmException,
			SignatureException, UnsupportedEncodingException,
			IllegalBlockSizeException, BadPaddingException,
			NoSuchPaddingException {
		SecretKeyAndCertificate skc = getMostRecent(publicKey);
		SecretKey secretKey = skc != null ? skc.secretKey : null;
		if (secretKey == null) {
			SecretKeyAndCertificate secretKeyAndCertificate = generateSecretKeyAndCertificate(
					publicKey, privateKey);
			putSecretKey(publicKey,
					secretKey = secretKeyAndCertificate.secretKey,
					secretKeyAndCertificate.skCert);
		}
		return secretKey;
	}

	@SuppressWarnings("serial")
	private static LinkedHashMap<Long, SecretKeyAndCertificate> newSecretKeyMap() {
		return new LinkedHashMap<Long, SecretKeyAndCertificate>() {

			protected boolean removeEldestEntry(
					@SuppressWarnings("rawtypes") Map.Entry eldest) {
				return size() > maxSecretKeys;
			}
		};
	}

	private synchronized static void putSecretKey(PublicKey publicKey,
			SecretKey secretKey, SecretKeyCertificate skCert) {
		assert (skCert.publicKey.equals(publicKey));
		if (!pk2sks.containsKey(publicKey))
			pk2sks.put(publicKey, newSecretKeyMap());
		Map<Long, SecretKeyAndCertificate> skeys = pk2sks.get(publicKey);
		skeys.put(skCert.timestamp, new SecretKeyAndCertificate(secretKey,
				skCert));
		// put back again to ensure it doesn't get garbage-collected
		pk2sks.put(publicKey, skeys);
	}

	/**
	 * @param secretKey
	 * @param skCert
	 */
	public synchronized static void putSecretKey(SecretKey secretKey,
			SecretKeyCertificate skCert) {
		putSecretKey(skCert.publicKey, secretKey, skCert);
	}

	private static final long SEND_SKCERT_THRESHOLD = Long.MAX_VALUE;

	/**
	 * @param skc
	 * @return True if the secret key certificate should be sent to the remote
	 *         endpoint along with the secret-key-signed message.
	 */
	public synchronized static boolean shouldSendSecretKey(
			SecretKeyAndCertificate skc) {
		return (System.currentTimeMillis() - skc.skCert.timestamp < SEND_SKCERT_THRESHOLD);
	}

	/**
	 * @param skCert
	 * @return Decrypts and returns the secret key contained in the supplied
	 *         secret key certificate.
	 * @throws SignatureException
	 * @throws NoSuchAlgorithmException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeyException
	 */
	public static SecretKey verify(SecretKeyCertificate skCert)
			throws SignatureException, NoSuchAlgorithmException,
			IllegalBlockSizeException, BadPaddingException,
			NoSuchPaddingException, InvalidKeyException {
		Cipher cipher = Cipher.getInstance(pkPair);
		cipher.init(Cipher.DECRYPT_MODE, skCert.publicKey);

		byte[] decrypted = cipher.doFinal(skCert.encryptedSecretKey);
		long timestamp = ByteBuffer.wrap(decrypted,
				decrypted.length - Long.BYTES, Long.BYTES).getLong();
		if (Math.abs(System.currentTimeMillis() - timestamp) < secretKeyLifetime)
			return new SecretKeySpec(decrypted, 0, decrypted.length
					- Long.BYTES, skEncryption);
		throw new RuntimeException("Secret key expired");
	}

	/**
	 * @param publicKey
	 * @return SecretKeyCertificate corresponding to publicKey. This method will
	 *         return the most recent if multiple certificates exist.
	 */
	public static SecretKeyCertificate getSecretKeyCertificate(
			PublicKey publicKey) {
		SecretKeyAndCertificate skc = getMostRecent(publicKey);
		return skc.skCert;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Util.assertAssertionsEnabled();
		Result result = JUnitCore.runClasses(SessionKeysTest.class);
		for (Failure failure : result.getFailures())
			System.out.println(failure.toString());
	}
}
