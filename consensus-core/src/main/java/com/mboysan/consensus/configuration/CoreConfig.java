package com.mboysan.consensus.configuration;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.ConfigCache;
import org.aeonbits.owner.ConfigFactory;

import java.util.Properties;

@Config.Sources("classpath:application.properties")
public interface CoreConfig extends Config {

    static <T extends CoreConfig> T newInstance(Class<T> configClass, Properties... properties) {
        return ConfigFactory.create(configClass, properties);
    }

    static <T extends CoreConfig> T getCached(Class<T> configClass, Properties... properties) {
        return ConfigCache.getOrCreate(configClass, properties);
    }
}
