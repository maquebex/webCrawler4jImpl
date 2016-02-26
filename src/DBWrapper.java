import java.util.ArrayList;
import java.util.HashMap;

import org.bson.Document;

import com.mongodb.Block;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

public class DBWrapper {
	
	public HashMap<String,HashMap<String,String>> fetchOne(MongoCollection<Document> collection, int id){
		/** source https://docs.mongodb.org/getting-started/java/query/
		 * #query-for-all-documents-in-a-collection
		 **/
		HashMap<String,HashMap<String,String>> record = new HashMap<String,HashMap<String,String>>();
		
		FindIterable<Document> iterable = collection.find(Filters.eq("_uid",id));
		iterable.limit(1);
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	HashMap<String,String> recordValues = new HashMap<String, String>();
		        recordValues.put("URL",document.getString("URL"));
		        recordValues.put("HTML_RES",document.getString("HTML_RES"));
		        recordValues.put("TEXT_RES",document.getString("TEXT_RES"));
		        recordValues.put("DOMAIN",document.getString("DOMAIN"));
		        recordValues.put("SUBDOMAIN",document.getString("SUBDOMAIN"));
		        recordValues.put("NUM_WORDS",document.getInteger("NUM_WORDS").toString());
		    	record.put(document.getString("URL"), recordValues);		    	
		    }
		});
		
		return record;
	}
	
	public void saveIndexBlock(MongoCollection<Document> indexEntry, HashMap<String, ArrayList<InvertedIndexEntry>> invertedIndexItems){
		for(String term:invertedIndexItems.keySet()){
			Document doc = new Document();
			doc.append("term", term);
			ArrayList<InvertedIndexEntry> invertedIndexItem = invertedIndexItems.get(term);
			ArrayList<Document> docList = new ArrayList<Document>();
			for(InvertedIndexEntry invertedIndex: invertedIndexItem){
				Document listdoc = new Document();				
				listdoc.append("doc_id", invertedIndex.getDocId());
				listdoc.append("tf", invertedIndex.getTermFrequency());
				listdoc.append("positions", invertedIndex.getTermPositions());
				docList.add(listdoc);
			}
			doc.append("docs", docList);
			try{
				FindIterable<Document> entry = indexEntry.find(Filters.eq("term",term)).limit(1);
				if(entry.first()==null){
					System.out.println("Insert");
					indexEntry.insertOne(doc);
					
				} else {
					System.out.println("Update");
					entry.forEach(new Block<Document>() {
					    @Override
					    public void apply(final Document document) {
					    	@SuppressWarnings("unchecked")
							ArrayList<Document> doclisting =(ArrayList<Document>) document.get("docs");
					    	doclisting.addAll(docList);
					    	indexEntry.findOneAndUpdate(Filters.eq("term",term), new Document("$set",document.append("docs",doclisting)));  			    	
					    }
					});
				}
				
			} catch(Exception e){
				e.printStackTrace();
			}
		}
		
		System.out.println("Done");
	}
}
