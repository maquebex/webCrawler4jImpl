import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

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
		        recordValues.put("_uid", document.getInteger("_uid").toString());
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
	
	public ArrayList<HashMap<String,String>> findIncomingPages(MongoCollection<Document> collection, String url){
		ArrayList<HashMap<String,String>> recordList = new ArrayList<HashMap<String,String>>();
		FindIterable<Document> iterable = collection.find(Filters.elemMatch("OUTGOING_LINKS", new Document("$eq", url)));
		iterable.forEach(new Block<Document>() {
		    @Override
		    public void apply(final Document document) {
		    	HashMap<String,String> recordValues = new HashMap<String, String>();
		        recordValues.put("URL",document.getString("URL"));
		        recordValues.put("HTML_RES",document.getString("HTML_RES"));
		        recordValues.put("docid", document.getInteger("_uid").toString());		        
		    	recordList.add(recordValues);		    	
		    }
		});
		return recordList;
	}
	
	public void updateInvertedIndex(MongoCollection<Document> collection, IndexEntry indexEntry){
		HashSet<String> tagCloud = indexEntry.aTags;
		for(String tag: tagCloud){
			try{
				FindIterable<Document> entry = collection.find(Filters.and(Filters.eq("term",tag),Filters.elemMatch("docs", new Document("doc_id",indexEntry.docId))));
				if(entry.first()==null){
					FindIterable<Document> termEntry = collection.find(Filters.eq("term",tag)).limit(1);
					System.out.println("doc id not found - Update "+tag +" to "+indexEntry.docId);
					termEntry.forEach(new Block<Document>() {
						@Override
						public void apply(final Document document) {
							@SuppressWarnings("unchecked")
							ArrayList<Document> doclisting =(ArrayList<Document>) document.get("docs");
							doclisting.add(new Document("doc_id",indexEntry.docId).append("tf", 1));
							collection.findOneAndUpdate(Filters.eq("term",tag), new Document("$set",document.append("docs",doclisting)));  			    	
						}
					});
				}				
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}
	
	public void updateIndex(MongoCollection<Document> collection, IndexEntry indexEntry){
		System.out.println("update "+indexEntry.docId);
		collection.findOneAndUpdate(Filters.eq("_uid",indexEntry.docId), new Document("$set",new Document("tag_cloud_id", indexEntry.indegreeDocIds)));
		
	/*	collection.findOneAndUpdate(Filters.eq("_uid",indexEntry.docId), new Document("$set",new Document("title",indexEntry.title)
																										.append("h1", indexEntry.h1Tags)
																										.append("h2", indexEntry.h2Tags)
																										.append("h3", indexEntry.h3Tags)
																										.append("b", indexEntry.bTags)
																										.append("meta", indexEntry.metaTags)));*/
	}
	
	public void createAnchorCollection(MongoCollection<Document> linkCollection, ArrayList<LinkEntry> linkData){
		for(LinkEntry l: linkData){
			ArrayList<Integer> docIds = new ArrayList<Integer>();
			ArrayList<String> links = new ArrayList<String>();
			Document doc = new Document();
			
			links.add(l.link);
			docIds.add(l.docId);
			
			doc.append("term", l.term);
			doc.append("docIds", docIds);
			doc.append("links", links);
			
			System.out.println("link record "+doc.toJson());
			
			try{
				FindIterable<Document> entry = linkCollection.find(Filters.eq("term",l.term)).limit(1);
				if(entry.first()==null){
					System.out.println("Insert");
					linkCollection.insertOne(doc);
					
				} else {
					System.out.println("Update");
					entry.forEach(new Block<Document>() {
					    @Override
					    public void apply(final Document document) {
					    	@SuppressWarnings("unchecked")
					    	ArrayList<Integer> docSet =(ArrayList<Integer>) document.get("docIds");
					    	@SuppressWarnings("unchecked")
					    	ArrayList<String> linkSet =(ArrayList<String>) document.get("links");
					    	HashSet<Integer> uniqueDocs = new HashSet<Integer>();
					    	HashSet<String> uniqueLinks = new HashSet<String>();
					    	
					    	docSet.addAll(docIds);
					    	linkSet.addAll(links);
					    	uniqueDocs.addAll(docSet);
					    	uniqueLinks.addAll(linkSet);				    	
					    	
					    	linkCollection.findOneAndUpdate(Filters.eq("term",l.term), new Document("$set",document.append("docIds",uniqueDocs).append("links", uniqueLinks)));  			    	
					    }
					});
				}				
			} catch(Exception e){
				e.printStackTrace();
			}
		}
	}
}
