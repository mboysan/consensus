package com.mboysan.consensus.message;

public abstract class RoutableRequest extends Message {

    /**
     * No routing needed. The receiver node is the target node.
     */
    public static final int ROUTE_TO_SELF = -1;

    /**
     * Asks the receiver node to route the request to this id. For instance, if the {@link #receiverId}=0 and
     * {@link #routeTo}=1, then node-0 will route the request to node-1 internally. And the client will still receive
     * the response from node-0.
     */
    private int routeTo;

    public RoutableRequest(int routeTo) {
        this.routeTo = routeTo;
    }

    public int getRouteTo() {
        return routeTo;
    }

    public boolean isRoutingNeeded() {
        return routeTo != ROUTE_TO_SELF;
    }

    public void resetRoutingState() {
        this.routeTo = ROUTE_TO_SELF;
    }

    @Override
    public String toString() {
        return "RoutableRequest{" +
                "routeTo=" + routeTo +
                "} " + super.toString();
    }
}
