package com.mboysan.consensus;

import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.Message;
import com.mboysan.consensus.message.SimMessage;

import java.io.IOException;

public interface SimRPC extends RPCProtocol {

    SimMessage simulate(SimMessage message) throws IOException;

    CustomResponse customRequest(CustomRequest request) throws IOException;

    @Override
    default Message processRequest(Message message) throws IOException {
        if (message instanceof SimMessage request) {
            return simulate(request);
        }
        if (message instanceof CustomRequest request) {
            return customRequest(request);
        }
        throw new IllegalArgumentException("unrecognized message=" + message);
    }

}
