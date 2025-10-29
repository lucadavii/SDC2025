package utils;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;

public class PublicKeyHelper {

	public static PublicKey rebuildPublicKey(byte[] encodedKey, String algorithm) throws Exception {
		X509EncodedKeySpec keySpec = new X509EncodedKeySpec(encodedKey);
		KeyFactory keyFactory = KeyFactory.getInstance(algorithm); 
		return keyFactory.generatePublic(keySpec);
	}
	
	public static PublicKey rebuildPublicKey(byte[] endodedKey) throws Exception {
		return PublicKeyHelper.rebuildPublicKey(endodedKey, "RSA");
	}

}
