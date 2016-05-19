package config;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class ReadProperties {
	
	public static Properties getProp() throws IOException {
        Properties props = new Properties();
        FileInputStream file = new FileInputStream(
                "./configExperiment.properties");
        props.load(file);
        return props;
 
    }
 
    public static void  main(String args[]) throws IOException {
         
        Properties prop = getProp();
        
        String host = prop.getProperty("mysql.host");
        String password = prop.getProperty("mysql.password");
        String db = prop.getProperty("mysql.db");
         
        System.out.println("Login = " + db);
        System.out.println("Host = " + host);
        System.out.println("Password = " + password);
    }

}
