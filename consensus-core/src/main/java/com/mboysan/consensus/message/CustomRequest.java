package com.mboysan.consensus.message;

public class CustomRequest extends Message {
    private final String request;
    /**
     * Asks the receiver node to route the request to this id. For instance, if the {@link #receiverId}=0 and
     * {@link #routeTo}=1, then node-0 will route the request to node-1 internally. And the client will still receive
     * the response from node-0.
     */
    private int routeTo = -1;

    public CustomRequest(String request) {
        this.request = request;
    }

    public String getRequest() {
        return request;
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
                ", routeTo=" + routeTo +
                "} " + super.toString();
    }
}
