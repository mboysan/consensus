package com.mboysan.consensus.util;

import com.mboysan.consensus.configuration.CliClientConfig;
import com.mboysan.consensus.configuration.NodeConfig;
import com.mboysan.consensus.configuration.TcpTransportConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public final class CliArgsHelper {

    private static final Logger LOGGER = LoggerFactory.getLogger(CliArgsHelper.class);

    static final Map<String, String> ALIAS_MAP = new HashMap<>();

    static {
        ALIAS_MAP.put("protocol", NodeConfig.Param.NODE_CONSENSUS_PROTOCOL);
        ALIAS_MAP.put("port", TcpTransportConfig.Param.SERVER_PORT);
        ALIAS_MAP.put("destinations", TcpTransportConfig.Param.DESTINATIONS);
        ALIAS_MAP.put("callbackTimeoutMs", TcpTransportConfig.Param.MESSAGE_CALLBACK_TIMEOUT_MS);
        ALIAS_MAP.put("cmd", CliClientConfig.Param.COMMAND);
        ALIAS_MAP.put("args", CliClientConfig.Param.ARGUMENTS);
        ALIAS_MAP.put("arg", CliClientConfig.Param.ARGUMENTS);
        ALIAS_MAP.put("to", CliClientConfig.Param.ROUTE_TO);
        ALIAS_MAP.put("k", CliClientConfig.Param.KEY);
        ALIAS_MAP.put("v", CliClientConfig.Param.VALUE);
        ALIAS_MAP.put("val", CliClientConfig.Param.VALUE);
    }

    private CliArgsHelper() {
    }

    public static void logArgs(String[] args) {
        LOGGER.info("Program arguments received=[{}]", String.join(" ", args));
    }

    public static Properties getProperties(String[] args) {
        Properties properties = new Properties();
        for (String arg : args) {
            addProperty(arg, properties);
        }
        return properties;
    }

    public static Properties getNodeSectionProperties(String[] args) {
        return getSectionProperties(args, "--node");
    }

    public static Properties getStoreSectionProperties(String[] args) {
        // populate missing properties
        Properties properties = getNodeSectionProperties(args);
        // override node section properties with the ones present in store section (e.g. port will be overridden)
        properties.putAll(getSectionProperties(args, "--store"));
        return properties;
    }

    private static Properties getSectionProperties(String[] args, String sectionName) {
        Properties properties = new Properties();
        boolean inSection = false;
        for (String arg : args) {
            if (arg.equals(sectionName)) {
                inSection = true;
                continue;
            }
            if (inSection) {
                if (arg.startsWith("--")) {
                    // new section
                    inSection = false;
                    continue;
                }
                addProperty(arg, properties);
            }
        }
        return properties;
    }

    private static void addProperty(String arg, Properties properties) {
        String[] kv = arg.split("=");
        if (kv.length != 2) {
            return;
        }
        String key = kv[0];
        String value = kv[1];

        if (key.equals("propsFile")) {
            addPropertiesFromFile(value, properties);
            return;
        }

        String realArg = ALIAS_MAP.get(key);
        if (realArg != null) {
            key = realArg;
        }
        properties.put(key, value);
    }

    private static void addPropertiesFromFile(String pathToPropertiesFile, Properties properties) {
        try (InputStream input = getInputStream(pathToPropertiesFile)) {
            properties.load(input);
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
            throw new UncheckedIOException(e);
        }
    }

    private static InputStream getInputStream(String pathToFile) throws FileNotFoundException {
        if (pathToFile.startsWith("classpath:")) {
            return CliArgsHelper.class.getClassLoader()
                    .getResourceAsStream(pathToFile.substring(pathToFile.indexOf(":") + 1));
        }
        return new FileInputStream(pathToFile);
    }

}
