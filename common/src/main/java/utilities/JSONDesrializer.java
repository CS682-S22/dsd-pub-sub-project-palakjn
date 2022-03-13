package utilities;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Responsible for serializing JSON string to object
 *
 * @author Palak Jain
 */
public class JSONDesrializer {
    private static final Logger logger = LogManager.getLogger(JSONDesrializer.class);

    /**
     * Parse JSON string into an object
     * @param body JSON String in bytes
     * @param classOfT Type of Class
     * @return Parsed object
     */
    public static <T> T fromJson(byte[] body, Class<T> classOfT) {
        Gson gson = new Gson();

        T object = null;

        try {
            String json = new String(body);
            object = gson.fromJson(json, classOfT);
        }
        catch (JsonSyntaxException exception) {
            logger.error("Unable to parse json", exception);
        }

        return object;
    }
}