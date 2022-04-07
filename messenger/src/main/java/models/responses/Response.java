package models.responses;

import com.google.gson.annotations.Expose;
import configuration.Constants;
import models.Object;
import utilities.Strings;

/**
 * Responsible for holding attributes to be sent as part of response of the request.
 *
 * @author Palak Jain
 */
public class Response<T> extends Object {
    @Expose
    private String status;
    @Expose
    private String message;
    @Expose
    private T object;

    public Response(String status, String message, T object) {
        this.status = status;
        this.message = message;
        this.object = object;
    }

    public Response(String status, T object) {
        this.status = status;
        this.object = object;
    }

    public Response(String status) {
        this.status = status;
    }

    /**
     * Get the message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Get the object being passed as response
     */
    public T getObject() {
        return object;
    }

    /**
     * Get the status of the response
     */
    public String getStatus() {
        return status;
    }

    /**
     * Checks if the response is success
     */
    public boolean isOk() {
        return status.equalsIgnoreCase(Constants.RESPONSE_STATUS.OK);
    }

    /**
     * Checks if the response indicating error
     */
    public boolean hasError() {
        return status.equalsIgnoreCase(Constants.RESPONSE_STATUS.ERROR);
    }

    /**
     * Checks if the election process is going on
     */
    public boolean isInSync() {
        return status.equalsIgnoreCase(Constants.RESPONSE_STATUS.SYN);
    }

    /**
     * Checks whether the response is valid or invalid
     */
    public boolean isValid() {
        return !Strings.isNullOrEmpty(status) && Constants.RESPONSE_STATUS.STATUS_LIST.contains(status) && object != null;
    }
}
