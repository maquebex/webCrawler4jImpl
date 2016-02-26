import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

public class MongoDBConnector implements Runnable{
	String collectionName;
	String counterCollectionName;
	Map<String, Object> responseBody;
	
	private static MongoCollection<Document> countersCollection;
	private static MongoCollection<Document> crawlerCollection;
	
	public MongoDBConnector(){
		
	}
	
	public MongoDBConnector(String collection, String counterCol, Map<String, Object> body) {
		counterCollectionName = counterCol;
		collectionName = collection;
		responseBody = body;
		
		crawlerCollection = Constants.db.getCollection(collectionName);
		countersCollection = Constants.db.getCollection(counterCollectionName);
		if (countersCollection.count() == 0) {
			createCountersCollection();
		}

	}
	
	void insertDocument(){
		try{
			Document doc= new Document(responseBody);
			doc.append("_uid", getNextSequence("URL_ID"));
			crawlerCollection.insertOne(doc);
			System.out.println("************here************** "+ System.currentTimeMillis());			
		} catch (Exception e){
			e.printStackTrace();
		}
	}
	
	public static void createCountersCollection() {

	    Document document = new Document();
	    document.append("_id", "URL_ID");
	    document.append("seq", 1);
	    countersCollection.insertOne(document);
	}	
	
	public static Object getNextSequence(String name) {

	    Document searchQuery = new Document("_id", name);
	    Document increase = new Document("seq", 1);
	    Document updateQuery = new Document("$inc", increase);
	    Document result = countersCollection.findOneAndUpdate(searchQuery, updateQuery);

	    return result.get("seq");
	}
	
	public void closeConnection(){
		Constants.mongoClient.close();
	}

	@Override
	public void run() {
		insertDocument();
	}
}
