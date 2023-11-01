package com.example.datamart.repository;

import com.mongodb.bulk.BulkWriteResult;
import org.bson.Document;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;

import org.springframework.stereotype.Repository;
import java.util.*;

/**
 * @author QuangNN
 */
@Repository
public class MongoRepository {
    @Autowired
    private MongoTemplate mongoTemplate;

    public void saveDocument(Document document, String collectionName) {
        mongoTemplate.insert(document, collectionName);
    }
    public void insertManyDocuments(List<Document> documents, String collectionName) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED, collectionName);
        for (Document document : documents) {
            bulkOperations.insert(document);
        }
    }
}
