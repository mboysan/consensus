package com.mboysan.consensus;

public interface CoreConstants {

    interface Metrics {
        String INSIGHTS_STORE_SIZE_OF_KEYS = "insights.store.sizeOf.keys";
        String INSIGHTS_STORE_SIZE_OF_VALUES = "insights.store.sizeOf.values";
        String INSIGHTS_STORE_SIZE_OF_TOTAL = "insights.store.sizeOf.total";
        String INSIGHTS_TCP_CLIENT_SEND_SIZEOF = "insights.tcp.client.send.sizeOf.%s";
        String INSIGHTS_TCP_CLIENT_RECEIVE_SIZEOF = "insights.tcp.client.receive.sizeOf.%s";
        String INSIGHTS_TCP_SERVER_SEND_SIZEOF = "insights.tcp.server.send.sizeOf.%s";
        String INSIGHTS_TCP_SERVER_RECEIVE_SIZEOF = "insights.tcp.server.receive.sizeOf.%s";
    }
}
