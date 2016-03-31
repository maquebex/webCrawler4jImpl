import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Set;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;

public class DatabaseConnector {

	MongoClient mongoclient;
	MongoDatabase database;
	MongoCollection<Document> index1;
	MongoCollection<Document> pages;
	MongoCollection<Document> pageRanks;
	
	public DatabaseConnector() {

		mongoclient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		database = mongoclient.getDatabase("crawler");

		index1 = database.getCollection("inverted_index");
		pages = database.getCollection("webcrawler_data");
		pageRanks = database.getCollection("PageRanks");
	}
	
	public Document getIndexEntry(String term)
	{
		FindIterable<Document> iterable = index1.find(new Document("term", term));
		MongoCursor<Document> iterator = iterable.iterator();
		return iterator.next();
	}
	
	public long getNumberOfDocuments()
	{
		return pages.count();
	}

	public PageContent getPageContentFromDB(Integer id) {
		FindIterable<Document> iterable = pages.find(new Document("_uid", id));
		MongoCursor<Document> iterator = iterable.iterator();
		Document d = iterator.next();
		PageContent p = new PageContent();
		p.setUrl(d.getString("URL"));
		p.setContent(d.getString("TEXT_RES"));
		p.setHtmlContent(d.getString("HTML_RES"));
		p.setDocId(d.getInteger(d.getInteger("_uid")));
		return p;
	}
	
	public HashMap<String, Page> getPageGraph()
	{
		HashMap<String, Page> pageGraph = new HashMap<>();
		FindIterable<Document> iterable = pages.find();
		MongoCursor<Document> iterator = iterable.iterator();		
		while(iterator.hasNext())
		{
			Document d = iterator.next();
			String url = d.getString("URL");
			Set<String> out = new HashSet<>();
			out.addAll((Collection<? extends String>) d.get("OUTGOING_LINKS"));
			System.out.println("Doc ID "+d.getInteger("_uid"));
			Page p = new Page(url, out,d.getInteger("_uid"));
			pageGraph.put(url, p);			
		}
		return pageGraph;
	}
	
	public void savePageRanks(HashMap<String, Double> pageRanks, HashMap<String, Integer> pageIds)
	{
		ArrayList<Document> list = new ArrayList<>();
		for(Entry<String, Double> e : pageRanks.entrySet())
		{
			Document d = new Document();
			d.put("Url", e.getKey());
			d.put("PageRank", e.getValue());
			d.append("docId", pageIds.get(e.getKey()));
			list.add(d);
		}
		this.pageRanks.insertMany(list);
	}
	
	public HashMap<String, Double> getPageRanks()
	{
		HashMap<String, Double> pageRanks = new HashMap<>();
		FindIterable<Document> iterable = this.pageRanks.find();
		MongoCursor<Document> iterator = iterable.iterator();
		while(iterator.hasNext())
		{
			Document d = iterator.next();
			pageRanks.put(d.getString("Url"), d.getDouble("PageRank"));
		}
		return pageRanks;
		
	}
}
