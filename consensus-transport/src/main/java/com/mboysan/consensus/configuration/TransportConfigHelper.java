package com.mboysan.consensus.configuration;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

public final class TransportConfigHelper {
    private TransportConfigHelper() {}

    public static Map<Integer, ? extends Destination> resolveDestinations(TransportConfig config) {
        if (config instanceof TcpTransportConfig conf) {
            return conf.destinations();
        }
        throw new IllegalArgumentException("unsupported config=" + config);
    }

    public static Set<Integer> resolveDestinationNodeIds(TransportConfig config) {
        return Collections.unmodifiableSet(resolveDestinations(config).keySet());
    }

    public static int resolveId(TransportConfig config) {
        if (config instanceof TcpTransportConfig conf) {
            return conf.destinations().values().stream()
                    .filter(dest -> dest.port() == conf.port())
                    .mapToInt(TcpDestination::nodeId)
                    .findFirst().orElse(conf.port());
        }
        throw new IllegalArgumentException("unsupported config=" + config);
    }
}
