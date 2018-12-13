package com.kumarad.dynamo;

public class ConditionalCheckException extends DynamoKeyValueException {
    public ConditionalCheckException(Throwable cause) {
        super(cause);
    }
}
