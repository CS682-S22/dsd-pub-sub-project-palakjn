package models.requests;

import com.google.gson.annotations.Expose;
import models.Object;

/**
 * Responsible for holding request params.
 *
 * @author Palak Jain
 */
public class Request<T> extends Object {
    @Expose
    private String type;
    @Expose
    private T request;

    public Request(T request) {
        this.request = request;
    }

    public Request(String type, T request) {
        this.type = type;
        this.request = request;
    }

    /**
     * Get the type of request
     */
    public String getType() {
        return type;
    }

    /**
     * Get the request
     */
    public T getRequest() {
        return request;
    }
}
