package Properties;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class get_properties {
    InputStream input;
    public Properties read() throws IOException {
        Properties prop = new Properties();

        try {
            String name = "api.properties";
            input = getClass().getClassLoader().getResourceAsStream(name);
            if (input != null) {
                prop.load(input);
            }
            else {
                throw new FileNotFoundException(name + " Check the classpath");
            }
        }
        catch(IOException ie) {
            ie.printStackTrace();
        }
        return prop;
    }
//    public static void main(String[] args) throws IOException {
//        Properties prop = new get_properties().read();
//        System.out.println(prop.getProperty("consumer_key"));
//        System.out.println(prop.getProperty("consumer_secret"));
//        System.out.println(prop.getProperty("access_token"));
//        System.out.println(prop.getProperty("access_token_secret"));
//        System.out.println(prop.getProperty("words_search_key"));
//    }
}


