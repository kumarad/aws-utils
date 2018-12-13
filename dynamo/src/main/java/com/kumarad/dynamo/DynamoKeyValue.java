package com.kumarad.dynamo;

import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBHashKey;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBTable;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBVersionAttribute;

import java.util.Objects;
import java.util.Optional;

/**
 * Used with DynamoDBMapper to read/write to Dynamo table.
 */
@DynamoDBTable(tableName="key-value-table")
public class DynamoKeyValue {
    private String key;
    private String value;
    private Long version;

    @DynamoDBHashKey
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    @DynamoDBVersionAttribute
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public static DynamoKeyValue fromKeyValue(KeyValue keyValue) {
        DynamoKeyValue dynamoKeyValue = new DynamoKeyValue();
        dynamoKeyValue.setKey(keyValue.getKey());
        dynamoKeyValue.setValue(keyValue.getValue());
        dynamoKeyValue.setVersion(keyValue.getVersion().orElse(null));
        return dynamoKeyValue;
    }

    public static KeyValue toKeyValue(DynamoKeyValue dynamoKeyValue) {
        KeyValue configKey = new KeyValue();
        return configKey.withKey(dynamoKeyValue.getKey())
                .withValue(dynamoKeyValue.getValue())
                .withVersion(Optional.of(dynamoKeyValue.getVersion()));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DynamoKeyValue that = (DynamoKeyValue) o;
        return Objects.equals(key, that.key) &&
                Objects.equals(value, that.value) &&
                Objects.equals(version, that.version);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key, value, version);
    }
}
