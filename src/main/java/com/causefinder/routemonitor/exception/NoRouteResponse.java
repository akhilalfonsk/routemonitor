package com.causefinder.routemonitor.exception;

public class NoRouteResponse extends RuntimeException {
    public NoRouteResponse(String cause) {
        super(cause);
    }
}
