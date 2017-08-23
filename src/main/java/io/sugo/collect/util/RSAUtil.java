package io.sugo.collect.util;

import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import javax.crypto.Cipher;

/**
 * RSA加解密工具
 * @author qiaopengfei
 * @date 2015年3月19日
 * @version 1.0.0
 * @Copyright (c) 2015, www.okii.com All Rights Reserved.
 */
public class RSAUtil {

	private static final String SIGN_TYPE_RSA = "RSA/ECB/PKCS1Padding";
	private static final String CHARSET = "UTF-8";
	private static final String hexStr = "0123456789ABCDEF";

	/**
	 * @param publicKey
	 * @return
	 * @throws Exception
	 */
	public static PublicKey getPublicKey(String publicKey) throws Exception {
		byte[] keyBytes = Base64.decode(publicKey);
		X509EncodedKeySpec x509 = new X509EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return keyFactory.generatePublic(x509);
	}

	/**
	 * @param privateKey
	 * @return
	 * @throws Exception
	 */
	private static PrivateKey getPrivateKey(String privateKey) throws Exception {
		byte[] keyBytes = Base64.decode(privateKey);
		PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(keyBytes);
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		return keyFactory.generatePrivate(keySpec);
	}

	/**
	 * 使用私钥加密
	 * @param content
	 * @param privateKey
	 * @return
	 * @throws Exception
	 */
	public static String priEncrypt(String content, String privateKey) throws Exception {
		Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
		cipher.init(Cipher.ENCRYPT_MODE, getPrivateKey(privateKey));
		byte[] signed = cipher.doFinal(content.getBytes(CHARSET));
		return Base64.encode(signed);
	}

	/**
	 * 使用公钥加密
	 * @param content
	 * @param publicKey
	 * @return
	 * @throws Exception
	 */
	public static String pubEncrypt(String content, String publicKey) throws Exception {
		Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
		cipher.init(Cipher.ENCRYPT_MODE, getPublicKey(publicKey));
		byte[] signed = cipher.doFinal(content.getBytes(CHARSET));
		return Base64.encode(signed);
	}

	/**
	 * 使用私钥解密
	 * @param content
	 * @param privateKey
	 * @return
	 * @throws Exception
	 */
	public static String priDecrypt(String content, String privateKey) throws Exception {
		long time = System.currentTimeMillis();
		Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
		cipher.init(Cipher.DECRYPT_MODE, getPrivateKey(privateKey));
		byte[] rs = cipher.doFinal(Base64.decode(content));
		System.out.println((System.currentTimeMillis() - time) + "获取chiper耗时");
		return new String(rs, CHARSET);
	}

	/**
	 * 使用公钥解密
	 * @param content
	 * @param publicKey
	 * @return
	 * @throws Exception
	 */
	public static String pubDecrypt(String content, String publicKey) throws Exception {
		Cipher cipher = Cipher.getInstance(SIGN_TYPE_RSA);
		cipher.init(Cipher.DECRYPT_MODE, getPublicKey(publicKey));
		byte[] rs = cipher.doFinal(Base64.decode(content));
		return new String(rs, CHARSET);
	}

	/**
	 * @param bytes
	 * @return 将二进制转换为十六进制字符输出
	 */
	public static String binaryToHexString(byte[] bytes) {

		String result = "";
		String hex = "";
		for (int i = 0; i < bytes.length; i++) {
			// 字节高4位
			hex = String.valueOf(hexStr.charAt((bytes[i] & 0xF0) >> 4));
			// 字节低4位
			hex += String.valueOf(hexStr.charAt(bytes[i] & 0x0F));
			result += "0x" + hex + ",";
		}
		return result;
	}

	public static void main(String[] args) throws Exception {
		String publicKey = "MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAL9n5AXhw1raL2B6O52LRKOcqjHydrFD4m+lFJW3xv/viRutOim4twKFlamB/edfz1KqydsMTVqsDCRiz8UuKU0CAwEAAQ==";
		String privateKey = "MIIBVAIBADANBgkqhkiG9w0BAQEFAASCAT4wggE6AgEAAkEAv2fkBeHDWtovYHo7nYtEo5yqMfJ2sUPib6UUlbfG/++JG606Kbi3AoWVqYH951/PUqrJ2wxNWqwMJGLPxS4pTQIDAQABAkAyRxj3jpEhSVTOk+0a+h1CmQF/8z/IWpudVhCtL0QfsBiQz1Jd75mr+SR9c59J+JGCo/FcdFQtbcCHq+m+VIHhAiEA7WmtE86WgWf3crIGEXAEnIGhowTP+gZcNkgwTZZmtGUCIQDOZB/G/GwbpU89xRXenNzGa6e0IgG5sDbSylxVcqkOyQIgDV/irDb7K5cbzY5R4TGaUObMoE5pGQC6uSQf9H8AkjECIHx6yOuz/OQjrQpdXxZX15RZA9nSFOZQ0JAL+uqScEJ5AiEAlqjUx7jNAbytpLqk5Us4lUFn2un9BoGAwX3BlMeoOgg=";

		String s = priEncrypt("123456789", privateKey);
		System.out.println(pubDecrypt("GrQSXFYJrVAmsPkucGK7pqC66s3uttSmKQIS/yUgDe6HV/LvFhaU84fu0CkuaSgU5Uqu/DtzudWtyc4e3diTvQ==", publicKey));
	}
}