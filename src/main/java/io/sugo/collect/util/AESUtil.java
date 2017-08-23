package io.sugo.collect.util;

import java.io.UnsupportedEncodingException;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/**
 * AES鍔犲瘑宸ュ叿
 * @author qiaopengfei
 * @date 2014骞�12鏈�4鏃�
 * @version 1.0.0
 * @Copyright (c) 2014, www.xtc.com All Rights Reserved.
 */
public class AESUtil {

	private static final String type = "AES";

	/**
	 * AES鍔犲瘑
	 * @param content
	 * @param password
	 * @return
	 * @throws Exception
	 * @throws UnsupportedEncodingException
	 * @throws Throwable
	 */
	public static String encryptAES(String content, String password) throws Exception {
		// 鏄庢枃鍔犲瘑鎴愬瓧鑺傚瘑鏂�
		byte[] encryptResult = encrypt(content.getBytes("utf-8"), password);
		return Base64.encode(encryptResult);
	}

	/**
	 * AES瑙ｅ瘑
	 * @param encryptResultStr
	 * @param password
	 * @return
	 * @throws Exception
	 * @throws Throwable
	 */
	public static String decryptAES(String encryptResultStr, String password) throws Exception {
		byte[] decryptFrom = Base64.decode(encryptResultStr);

		// 瀛楄妭瀵嗘枃杞槑鏂�
		byte[] decryptResult = decrypt(decryptFrom, password);
		return new String(decryptResult);
	}

	public static byte[] decryptAES(byte[] bytes, String password) throws Exception {
		byte[] decryptFrom = Base64.decode(new String(bytes, "utf-8"));
		// 瀛楄妭瀵嗘枃杞槑鏂�
		return decrypt(decryptFrom, password);
	}

	/**
	 * 鍔犲瘑
	 * @param bytes 闇�瑕佸姞瀵嗙殑鍐呭
	 * @param password 鍔犲瘑瀵嗙爜
	 * @return
	 */
	public static byte[] encrypt(byte[] bytes, String password) throws Exception {
		SecretKeySpec key = new SecretKeySpec(password.getBytes("UTF-8"), type);
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");// 鍒涘缓瀵嗙爜鍣�
		cipher.init(Cipher.ENCRYPT_MODE, key);// 鍒濆鍖�
		byte[] result = cipher.doFinal(bytes);
		return result; // 鍔犲瘑
	}

	/**
	 * 瑙ｅ瘑
	 * @param content 寰呰В瀵嗗唴瀹�
	 * @param password 瑙ｅ瘑瀵嗛挜
	 * @return
	 */
	public static byte[] decrypt(byte[] content, String password) throws Exception {
		SecretKeySpec key = new SecretKeySpec(password.getBytes("UTF-8"), type);
		Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");// 鍒涘缓瀵嗙爜鍣�
		cipher.init(Cipher.DECRYPT_MODE, key);// 鍒濆鍖�
		byte[] result = cipher.doFinal(content);
		return result; // 鍔犲瘑
	}

	public static String parseByte2HexStr(byte buf[]) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < buf.length; i++) {
			String hex = Integer.toHexString(buf[i] & 0xFF);
			if (hex.length() == 1) {
				hex = '0' + hex;
			}
			sb.append(hex.toUpperCase());
		}
		return sb.toString();
	}

	public static byte[] parseHexStr2Byte(String hexStr) {
		if (hexStr.length() < 1)
			return null;
		byte[] result = new byte[hexStr.length() / 2];
		for (int i = 0; i < hexStr.length() / 2; i++) {
			int high = Integer.parseInt(hexStr.substring(i * 2, i * 2 + 1), 16);
			int low = Integer.parseInt(hexStr.substring(i * 2 + 1, i * 2 + 2), 16);
			result[i] = (byte) (high * 16 + low);
		}
		return result;
	}

}
