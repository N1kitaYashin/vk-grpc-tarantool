package com.example.demo.kv;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "app.tarantool")
public class TarantoolProperties {

    private String host = "localhost";
    private int port = 3301;
    private String user = "guest";
    private String password = "";
    private String space = "KV";
    private int rangeBatchSize = 5_000;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getSpace() {
        return space;
    }

    public void setSpace(String space) {
        this.space = space;
    }

    public int getRangeBatchSize() {
        return rangeBatchSize;
    }

    public void setRangeBatchSize(int rangeBatchSize) {
        this.rangeBatchSize = rangeBatchSize;
    }
}
