package com.mboysan.consensus;

import com.mboysan.consensus.configuration.SimConfig;
import com.mboysan.consensus.message.CustomRequest;
import com.mboysan.consensus.message.CustomResponse;
import com.mboysan.consensus.message.SimMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mboysan.consensus.SimState.Role.LEADER;

public class SimNode extends AbstractNode<SimPeer> implements SimRPC {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimNode.class);
    private final SimConfig simConfig;
    private final SimClient rpcClient;
    private final SimState state;

    SimNode(SimConfig config, Transport transport) {
        super(config, transport);
        this.simConfig = config;
        this.rpcClient = new SimClient(transport);

        this.state = new SimState(getNodeId(), config.leaderId());
    }

    @Override
    Future<Void> startNode() {
        return CompletableFuture.supplyAsync(() -> {
            AtomicInteger count = new AtomicInteger(0);
            while (true) {
                count.set(0);
                forEachPeerParallel(peer -> {
                    CustomRequest request = new CustomRequest("sim_ping")
                            .setReceiverId(peer.peerId)
                            .setSenderId(getNodeId());
                    try {
                        getRPC().customRequest(request); // we aren't interested in the response
                        count.incrementAndGet();
                    } catch (IOException e) {
                        LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
                    }
                });
                if (count.get() == peers.size()) {
                    return null;
                }
                getScheduler().sleep(500);
            }
        });
    }

    @Override
    void update() {
        throw new UnsupportedOperationException("SimNode does not support update");
    }

    @Override
    SimPeer createPeer(int peerId) {
        // we don't add ourselves as a peer
        return peerId != getNodeId() ? new SimPeer(peerId) : null;
    }

    @Override
    SimRPC getRPC() {
        return rpcClient;
    }

    @Override
    public SimMessage simulate(SimMessage message) throws IOException {
        validateAction();
        state.getMessageReceiveCount().incrementAndGet();
        if (state.getRole().equals(LEADER)) {
            if (simConfig.broadcastToFollowers()) {
                forEachPeerParallel(peer -> {
                    SimMessage request = new SimMessage()
                            .setSenderId(getNodeId())
                            .setReceiverId(peer.peerId);
                    try {
                        getRPC().simulate(request); // we aren't interested in the response
                    } catch (IOException e) {
                        LOGGER.error("peer-{} IO exception for request={}, cause={}", peer.peerId, request, e.getMessage());
                    }
                });
            }
        } else {    // follower
            if (simConfig.forwardToLeader() && message.getSenderId() != state.getLeaderId()) {
                // forward message to leader
                return routeMessage(message, state.getLeaderId());
            }
        }
        // reply
        return new SimMessage();
    }

    @Override
    public CustomResponse customRequest(CustomRequest request) throws IOException {
        validateAction();
        if (request.getRouteTo() != -1) {
            int routeToId = request.getRouteTo();
            request.setRouteTo(-1);
            return getRPC().customRequest(request.setReceiverId(routeToId).setSenderId(getNodeId()));
        }
        synchronized (this) {
            if (CustomRequest.Command.CHECK_INTEGRITY.equals(request.getRequest())) {
                String stateStr = "node-" + getNodeId() + ": " + state.toString();
                return new CustomResponse(true, null, stateStr);
            }
            return new CustomResponse(
                    false, new UnsupportedOperationException(request.getRequest()), null);
        }
    }

    SimState getState() {
        return state;
    }
}
