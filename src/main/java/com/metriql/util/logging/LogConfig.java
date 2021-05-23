package com.metriql.util.logging;

import io.airlift.configuration.Config;

public class LogConfig {
    private boolean logActive = true;
    private String release;
    private String sentryDSN = "https://6d43d542d58c4ad6a6b0b834bda98100@o29344.ingest.sentry.io/1254747";

    public boolean getLogActive() {
        return logActive;
    }

    @Config("log.active")
    public LogConfig setLogActive(boolean logActive) {
        this.logActive = logActive;
        return this;
    }

    public String getRelease() { return release; }

    @Config("log.release")
    public LogConfig setRelease(String release) {
        this.release = release;
        return this;
    }

    public String getSentryDSN() { return sentryDSN; }

    @Config("log.sentry.dsn")
    public LogConfig setSentryDSN(String sentryDSN) {
        this.sentryDSN = sentryDSN;
        return  this;
    }
}
