package edu.umass.cs.utils;

import java.io.UnsupportedEncodingException;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.SignatureException;
import java.security.spec.InvalidKeySpecException;

import javax.crypto.BadPaddingException;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;

import org.junit.Assert;
import org.junit.Test;

import edu.umass.cs.utils.SessionKeys.SecretKeyCertificate;

/**
*
*/
public class SessionKeysTest extends DefaultTest {
	/**
	 * @throws NoSuchAlgorithmException
	 * @throws NoSuchPaddingException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 * @throws UnsupportedEncodingException
	 * @throws SignatureException
	 * @throws InvalidKeyException
	 */



	@Test
	public void test_SignVerifySecretKey() throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException,
			UnsupportedEncodingException, IllegalBlockSizeException,
			BadPaddingException, NoSuchPaddingException {
		Util.assertAssertionsEnabled();
		KeyPair keyPair = KeyPairGenerator.getInstance(SessionKeys.keyPairAlgorithm)
				.genKeyPair();
		SecretKey sk = SessionKeys.getOrGenerateSecretKey(keyPair.getPublic(),
				keyPair.getPrivate());
		SecretKeyCertificate skCert = SessionKeys
				.getSecretKeyCertificate(keyPair.getPublic());
		System.out.println(sk);
		System.out.println("secret_key_length=" + sk.getEncoded().length
				+ "; signed_secret_key_length="
				+ skCert.encryptedSecretKey.length);
		Assert.assertEquals(sk, (SessionKeys.verify(skCert)));
	}

	/**
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 */
	@Test
	public void test_Generate() throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException,
			UnsupportedEncodingException, IllegalBlockSizeException,
			BadPaddingException, NoSuchPaddingException {
		KeyPair keyPair = KeyPairGenerator.getInstance(SessionKeys.keyPairAlgorithm)
				.genKeyPair();
		SecretKey sk = SessionKeys.getOrGenerateSecretKey(keyPair.getPublic(),
				keyPair.getPrivate());
		Assert.assertEquals(SessionKeys.getSecretKey(keyPair.getPublic()), sk);
	}

	/**
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeySpecException
	 */
	@Test
	public void test_EncodeDecode() throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException,
			UnsupportedEncodingException, IllegalBlockSizeException,
			BadPaddingException, NoSuchPaddingException,
			InvalidKeySpecException {
		KeyPair keyPair = KeyPairGenerator.getInstance(SessionKeys.keyPairAlgorithm)
				.genKeyPair();
		SecretKey secretKey = SessionKeys.getOrGenerateSecretKey(
				keyPair.getPublic(), keyPair.getPrivate());
		SecretKeyCertificate skCert = SessionKeys
				.getSecretKeyCertificate(keyPair.getPublic());
		assert (skCert != null);
		System.out.println(skCert);
		byte[] encoded = skCert.getEncoded(true);
		SecretKey decoded = SessionKeys.getSecretKeyFromCertificate(encoded);
		Assert.assertEquals(secretKey, decoded);
		Assert.assertEquals(
				secretKey,
				SessionKeys.getSecretKeyFromCertificate(encoded,
						keyPair.getPublic()));
	}

	/**
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws SignatureException
	 * @throws UnsupportedEncodingException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 * @throws NoSuchPaddingException
	 * @throws InvalidKeySpecException
	 */
	@Test
	public void test_DecodePerformance() throws NoSuchAlgorithmException,
			InvalidKeyException, SignatureException,
			UnsupportedEncodingException, IllegalBlockSizeException,
			BadPaddingException, NoSuchPaddingException,
			InvalidKeySpecException {
		KeyPair keyPair = KeyPairGenerator.getInstance(SessionKeys.keyPairAlgorithm)
				.genKeyPair();
		SecretKey secretKey = SessionKeys.getOrGenerateSecretKey(
				keyPair.getPublic(), keyPair.getPrivate());
		SecretKeyCertificate skCert = SessionKeys
				.getSecretKeyCertificate(keyPair.getPublic());
		assert (skCert != null);
		System.out.println(skCert);
		byte[] encoded = skCert.getEncoded(true);
		Assert.assertEquals(
				secretKey,
				SessionKeys.getSecretKeyFromCertificate(encoded,
						keyPair.getPublic()));
		int n = 1000 * 1000;
		long t = System.currentTimeMillis();
		for (int i = 0; i < n; i++)
			SessionKeys.getSecretKeyFromCertificate(encoded,
					keyPair.getPublic());
		System.out.println("decode_secret_key_certificate_rate="
				+ Util.df(n * 1.0 / (System.currentTimeMillis() - t)) + "K/s");
		
		t = System.currentTimeMillis();
		for (int i = 0; i < n; i++)
			SessionKeys.getSecretKeyCertificate(
					keyPair.getPublic());
		System.out.println("get_secret_key_certificate_rate="
				+ Util.df(n * 1.0 / (System.currentTimeMillis() - t)) + "K/s");

	}
}
