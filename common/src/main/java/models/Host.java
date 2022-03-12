package models;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import configuration.Constants;
import utilities.Strings;

import java.nio.charset.StandardCharsets;

public class Host {
    private String address;
    private int port;

    public Host(String address, int port) {
        this.address = address;
        this.port = port;
    }

    public String getAddress() {
        return address;
    }

    public int getPort() {
        return port;
    }

    public boolean isValid() {
        return !(Strings.isNullOrEmpty(address) || port < Constants.START_VALID_PORT || port > Constants.END_VALID_PORT);
    }

    public String toString() {
        String stringFormat = null;

        try {
            Gson gson = new Gson();
            stringFormat = gson.toJson(this);
        } catch (JsonSyntaxException exception) {
            System.out.println("Unable to convert the Broker object to json");
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
