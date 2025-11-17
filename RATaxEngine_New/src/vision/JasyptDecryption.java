package vision;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;

public class JasyptDecryption {
	private static String mpCryptoPassword = "v!$!0n";

//	public static void main(String[] args) {
//		StandardPBEStringEncryptor encryptor = new StandardPBEStringEncryptor();
//		encryptor.setPassword(mpCryptoPassword);
//		encryptor.setAlgorithm("PBEWithSHA1AndDESede");
//
//		String encryptedPassword = "ENC(ydSPQIxmLlqeeySoWBonQIIzEh4jwvNC)";
//		encryptedPassword = encryptedPassword.substring(4, encryptedPassword.length() - 1);
//		System.out.println("encryptedPassword:" + encryptedPassword);
//
//		String decryptedPassword = encryptor.decrypt(encryptedPassword);
//
//		System.out.println("Decrypted password: " + decryptedPassword);
//
//	}
}
