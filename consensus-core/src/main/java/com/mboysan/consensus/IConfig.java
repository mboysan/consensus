package com.mboysan.consensus;

import org.aeonbits.owner.Config;
import org.aeonbits.owner.ConfigCache;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.Converter;

import java.lang.reflect.Method;
import java.util.Properties;

@Config.Sources("classpath:application.properties")
public interface IConfig extends Config {
    @Key("rng.seed")
    @DefaultValue("")
    @ConverterClass(SeedConverter.class)
    String rngSeed();

    class SeedConverter implements Converter<String> {
        @Override
        public String convert(Method method, String s) {
            if (s == null || s.trim().length() == 0) {
                // default
                return System.currentTimeMillis() + "";
            }
            return s;
        }
    }

    static <T extends IConfig> T newInstance(Class<T> configClass, Properties... properties) {
        return ConfigFactory.create(configClass, properties);
    }

    static <T extends IConfig> T getCached(Class<T> configClass, Properties... properties) {
        return ConfigCache.getOrCreate(configClass, properties);
    }
}
