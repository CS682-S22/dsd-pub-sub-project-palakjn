package models.requests;

import models.Object;

public class Request<T> extends Object {
    private T request;

    public Request(T request) {
        this.request = request;
    }

    public T getRequest() {
        return request;
    }
}
