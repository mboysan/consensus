package com.mboysan.consensus.message;

public class CustomRequest extends RoutableRequest {
    private final String request;
    private final String arguments;

    public CustomRequest(String request) {
        this(request, null);
    }

    public CustomRequest(String request, String arguments) {
        this(ROUTE_TO_SELF, request, arguments);
    }

    public CustomRequest(int routeTo, String request, String arguments) {
        super(routeTo);
        this.request = request;
        this.arguments = arguments;
    }

    public String getRequest() {
        return request;
    }

    public String getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return "CustomRequest{" +
                "request='" + request + '\'' +
                ", arguments='" + arguments + '\'' +
                "} " + super.toString();
    }
}
