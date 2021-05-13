package com.mboysan.dist;

import java.io.Serializable;
import java.util.function.Function;

public interface ProtocolRPC extends Function<Serializable, Serializable> {
    ProtocolRPC createClient(Transport transport, int senderId, int receiverId);
}
