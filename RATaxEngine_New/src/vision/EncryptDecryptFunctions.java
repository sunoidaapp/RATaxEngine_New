package vision;

import jcifs.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class EncryptDecryptFunctions {
	
	static final String SECRET = "Spiral Architect";
	
	public static String passwordDecrypt(String ciphertext) {
	       try{
	         byte[] secret = (SECRET.hashCode() + "").substring(0, 8).getBytes();
	               Cipher des = Cipher.getInstance("DES");
	               des.init(Cipher.DECRYPT_MODE, new SecretKeySpec(secret, "DES"));
	               byte[] plaintext = des.doFinal(Base64.decode(ciphertext));
	               return new String(plaintext);
	        }catch(Exception e){
	         e.printStackTrace();
	        }
	        return ciphertext;
		 }

	public static String passwordEncrypt(String plaintext) {
		try {
			byte[] secret = (SECRET.hashCode() + "").substring(0, 8).getBytes();
			Cipher des = Cipher.getInstance("DES");
			des.init(Cipher.ENCRYPT_MODE, new SecretKeySpec(secret, "DES"));
			byte[] ciphertext = des.doFinal(plaintext.getBytes());
			return Base64.encode(ciphertext);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return plaintext;
	}
	public static String jaspytPasswordDecrypt(String encryptedPwd,String secretKey) {
		String decryptedPwd = "";
		try {
			StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
	    	encryptor.setPassword(secretKey);
	        encryptor.setAlgorithm("PBEWithSHA1AndDESede");
			encryptedPwd = encryptedPwd.substring(4, encryptedPwd.length()-1);
			decryptedPwd = encryptor.decrypt(encryptedPwd);
		}catch(Exception e) {
			e.printStackTrace();
		}
		return decryptedPwd;
	}
	 
}
