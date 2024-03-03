package com.mboysan.consensus.message;

public class CustomRequest extends Message {

    public interface Command {
        String CHECK_INTEGRITY = "checkIntegrity";
    }

    private final String request;
    private final String arguments;
    /**
     * Asks the receiver node to route the request to this id. For instance, if the {@link #receiverId}=0 and
     * {@link #routeTo}=1, then node-0 will route the request to node-1 internally. And the client will still receive
     * the response from node-0.
     */
    private int routeTo = -1;

    public CustomRequest(String request) {
        this(request, null);
    }

    public CustomRequest(String request, String arguments) {
        this.request = request;
        this.arguments = arguments;
    }

    public String getRequest() {
        return request;
    }

    public String getArguments() {
        return arguments;
    }

    public int getRouteTo() {
        return routeTo;
    }

    public CustomRequest setRouteTo(int nodeId) {
        this.routeTo = nodeId;
        return this;
    }

    @Override
    public String toString() {
        return "CustomRequest{" +
                "request='" + request + '\'' +
                ", arguments='" + arguments + '\'' +
                ", routeTo=" + routeTo +
                "} " + super.toString();
    }
}
