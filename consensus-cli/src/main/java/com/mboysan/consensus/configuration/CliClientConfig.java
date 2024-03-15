package com.mboysan.consensus.configuration;

public interface CliClientConfig extends CoreConfig {

        interface Param {
            String INTERACTIVE = "interactive";
            String COMMAND = "command";
            String ARGUMENTS = "arguments";
            String ROUTE_TO = "routeTo";
            String KEY = "key";
            String VALUE = "value";
            String LEVEL = "level";
        }

        @Key(Param.INTERACTIVE)
        @DefaultValue("true")
        boolean interactive();

        @Key(Param.COMMAND)
        String command();

        @Key(Param.ARGUMENTS)
        String arguments();

        @Key(Param.KEY)
        String key();

        @Key(Param.VALUE)
        String value();

        @Key(Param.LEVEL)
        int level();

        @Key(Param.ROUTE_TO)
        @DefaultValue("-1")
        int routeTo();
}
