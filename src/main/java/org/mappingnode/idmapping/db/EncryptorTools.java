package org.mappingnode.idmapping.db;

import org.jasypt.encryption.StringEncryptor;
import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.EnvironmentPBEConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;

/**
 * <p> 加密工具类</p>
 * @author youq  2021/12/15 19:57
 */
@Configuration
public class EncryptorTools implements Ordered {

    @Value("${encryptor.password:Gstd@2021}")
    private String password;

    @ConditionalOnProperty(name = "encryptor.enable", havingValue = "true", matchIfMissing = true)
    @Bean
    public StringEncryptor stringEncryptor() {
        StandardPBEStringEncryptor standardPBEStringEncryptor = new StandardPBEStringEncryptor();
        EnvironmentPBEConfig config = new EnvironmentPBEConfig();
        config.setPassword(password);
        standardPBEStringEncryptor.setConfig(config);
        return standardPBEStringEncryptor;
    }

    public static String decrypt(StringEncryptor stringEncryptor, final String encodedValue) {
        if(stringEncryptor != null) {
            try {
                return stringEncryptor.decrypt(encodedValue);
            }catch(Exception e) {
                return encodedValue;
            }
        }else {
            return encodedValue;
        }
    }

    public static String encrypt(StringEncryptor stringEncryptor, final String plainVaue) {
        if(stringEncryptor != null) {
            try {
                return stringEncryptor.encrypt(plainVaue);
            }catch(Exception e) {
                return plainVaue;
            }
        }else {
            return plainVaue;
        }
    }

    public static String encrypt(String password, String plainText){
        StandardPBEStringEncryptor stringEncryptor = new StandardPBEStringEncryptor();
        EnvironmentPBEConfig config = new EnvironmentPBEConfig();
        config.setPassword(password);
        stringEncryptor.setConfig(config);
        return stringEncryptor.encrypt(plainText);
    }

    public static void main(String[] args) {
        System.out.println(encrypt("Gstd@2021", "5Q9Hc3bN3AjAdOZo"));
    }

    @Override
    public int getOrder() {
        return 0;
    }

}
