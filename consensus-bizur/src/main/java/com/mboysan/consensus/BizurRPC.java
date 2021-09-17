package com.mboysan.consensus;

import java.io.IOException;
import java.io.UncheckedIOException;

public interface BizurRPC extends RPCProtocol {
    HeartbeatResponse heartbeat(HeartbeatRequest request) throws IOException;

    PleaseVoteResponse pleaseVote(PleaseVoteRequest request) throws IOException;
    ReplicaReadResponse replicaRead(ReplicaReadRequest request) throws IOException;
    ReplicaWriteResponse replicaWrite(ReplicaWriteRequest request) throws IOException;

    KVGetResponse get(KVGetRequest request) throws IOException;
    KVSetResponse set(KVSetRequest request) throws IOException;

    @Override
    default Message apply(Message message) {
        try {
            if (message instanceof HeartbeatRequest) {
                return heartbeat((HeartbeatRequest) message);
            } else if (message instanceof PleaseVoteRequest) {
                return pleaseVote((PleaseVoteRequest) message);
            } else if (message instanceof ReplicaReadRequest) {
                return replicaRead((ReplicaReadRequest) message);
            } else if (message instanceof ReplicaWriteRequest) {
                return replicaWrite((ReplicaWriteRequest) message);
            } else if (message instanceof KVGetRequest) {
                return get((KVGetRequest) message);
            } else if (message instanceof KVSetRequest) {
                return set((KVSetRequest) message);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        throw new IllegalArgumentException("unrecognized message");
    }

    @Override
    default BizurRPC getRPC(Transport transport) {
        return new BizurClient(transport);
    }

}
