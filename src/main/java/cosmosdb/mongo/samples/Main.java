package cosmosdb.mongo.samples;

import com.google.common.base.Stopwatch;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoCommandException;
import com.mongodb.MongoQueryException;
import cosmosdb.mongo.samples.sdkextensions.CosmosDBBatchReader;
import cosmosdb.mongo.samples.sdkextensions.MongoAggregates;
import cosmosdb.mongo.samples.sdkextensions.MongoClientExtension;
import cosmosdb.mongo.samples.sdkextensions.RequestResponse;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.util.List;
import java.util.concurrent.Callable;

import static com.mongodb.client.model.Filters.gt;

public class Main {

    private static ConfigSettings configSettings = new ConfigSettings();
    private static MongoClientExtension mongoClientExtension;
    private static int MaxRetries = 10;

    public static void main(final String[] args) throws Exception {

        configSettings.Init();
        InitMongoClient36();

        long start = System.currentTimeMillis();
        ExecuteMethod(new Callable<Void>() {
            public Void call() throws Exception {
                ReadBigFile(1);
                return null;
            }
        });

        ExecuteMethod(new Callable<Void>() {
            public Void call() throws Exception {
                PrintCount();
                return null;
            }
        });

    }

    /*
    Demonstrates how to use $sum using aggregation pipeline.
    Also show how to convert different data types to integer.
     */
    private static void PrintCount() throws Exception {
        Bson filter = gt("AP_DERIVED_AMOUNT", 1000);
        BsonDocument findFilter = filter.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());
        int count = MongoAggregates.GetCountByFilter(mongoClientExtension, configSettings, new BasicDBObject("$match", findFilter), true, 10);

    }

    private static void ReadBigFile(int noOfRuns) throws Exception {

        Bson filter = gt("AP_DERIVED_AMOUNT", 1000);
        BsonDocument findFilter = filter.toBsonDocument(BsonDocument.class, MongoClient.getDefaultCodecRegistry());

        for (int i = 0; i < noOfRuns; i++) {
            RequestResponse<List<Document>, Document> output =
                    CosmosDBBatchReader.GetDocumentsUsingAggregationFilterWithRetries(
                            mongoClientExtension,
                            configSettings,//null,
                            new BasicDBObject("$match", findFilter),
                            1000,
                            false,
                            50,
                            true);
            System.out.println("Received documents count: " + output.Result.size());
        }
    }


    private static void InitMongoClient36() {
        mongoClientExtension = new MongoClientExtension();
        mongoClientExtension.InitMongoClient36(
                configSettings.getUserName(),
                configSettings.getPassword(),
                10255,
                true,
                configSettings.getClientThreadsCount()
        );
    }

    public static void ExecuteMethod(
            Callable<Void> callable) throws Exception {
        long start = System.currentTimeMillis();
        callable.call();
        long finish = System.currentTimeMillis();
        System.out.println("Total time taken to execute this method in milliseconds : " + (finish - start));
    }
}

