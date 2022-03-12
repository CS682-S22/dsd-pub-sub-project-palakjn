package models;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import utilities.Strings;

import java.nio.charset.StandardCharsets;

public class Object {

    public String toString() {
        String stringFormat = null;

        try {
            Gson gson = new Gson();
            stringFormat = gson.toJson(this);
        } catch (JsonSyntaxException exception) {
            System.out.println("Unable to convert the object to json");
        }

        return stringFormat;
    }

    public byte[] toByte() {
        byte[] bytes = null;
        String json = toString();

        if (!Strings.isNullOrEmpty(json)) {
            bytes = json.getBytes(StandardCharsets.UTF_8);
        }

        return bytes;
    }
}
