package com.kumarad.dynamo;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapper;
import com.amazonaws.services.dynamodbv2.datamodeling.DynamoDBMapperConfig;
import com.amazonaws.services.dynamodbv2.model.ConditionalCheckFailedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class DynamoDao {

    private static Logger logger = LoggerFactory.getLogger(DynamoDao.class);
    private final DynamoDBMapper mapper;

    public DynamoDao(AmazonDynamoDB amazonDynamoDB, String environmentName) {
        DynamoDBMapperConfig config = DynamoDBMapperConfig.builder()
                .withTableNameOverride(DynamoDBMapperConfig.TableNameOverride.withTableNamePrefix(String.format("%s-", environmentName)))
                .withConsistentReads(DynamoDBMapperConfig.ConsistentReads.CONSISTENT)
                .build();
        mapper = constructDynamoDBMapper(amazonDynamoDB, config);

    }

    public KeyValue set(KeyValue keyValue) throws DynamoKeyValueException {
        DynamoKeyValue dynamoKeyValue = DynamoKeyValue.fromKeyValue(keyValue);
        try {
            mapper.save(dynamoKeyValue);

            // mapper.save updates the version in the dynamoKeyValue reference.
            return DynamoKeyValue.toKeyValue(dynamoKeyValue);
        } catch (ConditionalCheckFailedException e) {
            logger.warn(String.format("Conditional check error setting ConfigKeyValue %s", keyValue), e);
            throw new ConditionalCheckException(e);
        } catch (AmazonClientException e) {
            logger.error(String.format("Failed to set ConfigKeyValue %s", keyValue), e);
            throw new DynamoKeyValueException(e);
        }
    }

    public void delete(KeyValue keyValue) throws DynamoKeyValueException {
        DynamoKeyValue dynamoKeyValue = DynamoKeyValue.fromKeyValue(keyValue);
        try {
            mapper.delete(dynamoKeyValue);
        } catch (ConditionalCheckFailedException e) {
            logger.warn(String.format("Conditional check error deleting ConfigKeyValue %s", keyValue), e);
            throw new ConditionalCheckException(e);
        } catch (AmazonClientException e) {
            logger.error(String.format("Failed to delete ConfigKeyValue %s", keyValue), e);
            throw new DynamoKeyValueException(e);
        }
    }

    /**
     * Will go up the the subdomain/domain hierarchy if necessary when searching for a config value.
     */
    public Optional<KeyValue> get(String key) throws DynamoKeyValueException {
        try {
             DynamoKeyValue dynamoKeyValue = mapper.load(DynamoKeyValue.class, key);
             if (dynamoKeyValue != null) {
                return Optional.of(DynamoKeyValue.toKeyValue(dynamoKeyValue));
             } else {
                 return Optional.empty();
             }
        } catch (AmazonClientException e) {
            logger.error(String.format("Failed to load value for key %s", key), e);
            throw new DynamoKeyValueException(e);
        }
    }

    private DynamoDBMapper constructDynamoDBMapper(AmazonDynamoDB amazonDynamoDB, DynamoDBMapperConfig config) {
        return new DynamoDBMapper(amazonDynamoDB, config);
    }

}
