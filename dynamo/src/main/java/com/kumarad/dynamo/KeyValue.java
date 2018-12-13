package com.kumarad.dynamo;

import java.util.Optional;

public class KeyValue {
    private String key;
    private String value;
    private Optional<Long> version = Optional.empty();

    public KeyValue() {}

    public String getKey() {
        return key;
    }

    public KeyValue withKey(String key) {
        this.key = key;
        return this;
    }

    public String getValue() {
        return value;
    }

    public KeyValue withValue(String value) {
        this.value = value;
        return this;
    }

    public Optional<Long> getVersion() {
        return version;
    }

    public KeyValue withVersion(Optional<Long> version) {
        this.version = version;
        return this;
    }
}
