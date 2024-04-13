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

    interface StateLevels {
        /** Only integrity hash */
        int INFO_STATE = 1;
        /** Verbose state info */
        int DEBUG_STATE = 2;
        /** More verbose state info */
        int TRACE_STATE = 3;
        /** Integrity hash from all nodes */
        int INFO_STATE_FROM_ALL = 10;
        /** Verbose state from all nodes */
        int DEBUG_STATE_FROM_ALL = 20;
        /** More verbose state from all nodes */
        int TRACE_STATE_FROM_ALL = 30;
    }
}
