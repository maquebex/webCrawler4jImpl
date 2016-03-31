import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.bson.Document;

import com.mongodb.client.MongoCollection;

import edu.uci.ics.crawler4j.crawler.CrawlConfig;
import edu.uci.ics.crawler4j.crawler.CrawlController;
import edu.uci.ics.crawler4j.fetcher.PageFetcher;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtConfig;
import edu.uci.ics.crawler4j.robotstxt.RobotstxtServer;

public class Controller {
    public static void main(String[] args) throws Exception {
    	
        if(Constants.SHOULD_CRAWL){
        	String crawlStorageFolder = "data/crawl/root";
        	int numberOfCrawlers = 1;

        	CrawlConfig config = new CrawlConfig();
        	config.setCrawlStorageFolder(crawlStorageFolder);
        	config.setUserAgentString(Constants.USER_AGENT);
        	config.setPolitenessDelay(1000);
        	config.setResumableCrawling(true);//change to true
        	config.setMaxDownloadSize(1000000);

        	
        	 // Instantiate the controller for this crawl.
        	 
        	PageFetcher pageFetcher = new PageFetcher(config);
        	RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        	RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        	CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

        	
        	 /* For each crawl, you need to add some seed urls. These are the first
        	 * URLs that are fetched and then the crawler starts following links
        	 * which are found in these pages*/
        	 
        	controller.addSeed("http://www.ics.uci.edu/");

        	
        	 /* Start the crawl. This is a blocking operation, meaning that your code
        	 * will reach the line after this only when crawling is finished.*/
        	 
        	controller.start(MyCrawler.class, numberOfCrawlers);
        	controller.shutdown();	// comment if needed
        	controller.waitUntilFinish();
        	Constants.mongoClient.close(); // comment if needed
        
        } else {
        	// process crawled data
        	StringUtils stringUtils = new StringUtils();
        	DBWrapper dbwrapper = new DBWrapper();
        	MongoCollection<Document> collection = Constants.db.getCollection("webcrawler_data");
        	MongoCollection<Document> linkCollection = Constants.db.getCollection("link_data");
        	
        	for(int i=1;i<=Constants.DB_ROW_COUNT;i++){
        		HashMap<String,HashMap<String,String>> records =dbwrapper.fetchOne(collection,i);
        		//pageLinkAnalysis(records, i, stringUtils, dbwrapper,collection,linkCollection);
        		getPageFeaturesForRanking(records, i, stringUtils, dbwrapper,collection);
        		//generateInvertedIndex(records, i, stringUtils, dbwrapper);
        		
        	}
        	
        	System.out.println("Done");
        }  
    }
    
    public static void pageLinkAnalysis(HashMap<String,HashMap<String,String>> records, int docId, StringUtils stringUtils, DBWrapper dbwrapper, MongoCollection<Document> collection,MongoCollection<Document> linkCollection){
    	ArrayList<HashMap<String,String>> incomingLinkSet= new ArrayList<HashMap<String,String>>();
    	HashSet<Integer> docIds = new HashSet<Integer>();
    	HashSet<String> aTags = new HashSet<String>();
    	ArrayList<LinkEntry> linkArray = new ArrayList<LinkEntry>();
    	
    	String link="";
    	
    	for(String url:records.keySet()){
    		link=url;
    		incomingLinkSet = dbwrapper.findIncomingPages(collection,url);
    	}
    	
    	for(int i=0;i<incomingLinkSet.size();i++){
    		docIds.add(Integer.parseInt(incomingLinkSet.get(i).get("docid")));
    		//aTags.addAll(stringUtils.getAnchorData(incomingLinkSet.get(i).get("HTML_RES")));
    	}
    	
    	for(String atag:aTags){
    		LinkEntry linkEntry = new LinkEntry();
    		linkEntry.setTerm(atag);
    		linkEntry.setDocId(docId);
    		linkEntry.setLink(link);
    		linkArray.add(linkEntry);
    	}
    	System.out.println("Link entry "+ linkArray.toString());
    	dbwrapper.createAnchorCollection(linkCollection,linkArray);
    	
    	//uncomment this to add list of docs ids pointing to current doc in webcrawler_data
    	IndexEntry indexEntry = new IndexEntry();
    	indexEntry.setDocId(docId);
    	indexEntry.setIndegreeDocIds(docIds);
    	//indexEntry.setaTags(aTags);// this is to store link text (optional)
    	dbwrapper.updateIndex(collection, indexEntry);
    }
    
    public static void getPageFeaturesForRanking(HashMap<String,HashMap<String,String>> records, int docId, StringUtils stringUtils, DBWrapper dbwrapper, MongoCollection<Document> collection){
    	IndexEntry indexEntry = new IndexEntry();
    	
    	for(String url:records.keySet()){
    		String page =records.get(url).get("HTML_RES").toLowerCase();
			HashSet<String> title = stringUtils.getTitleTokens(page);
			HashSet<String> metaTags = stringUtils.getMetaData(page);
			HashSet<String> h1Tags = stringUtils.getH1(page);			
			HashSet<String> h2Tags = stringUtils.getH2(page);
			HashSet<String> h3Tags = stringUtils.getH3(page);
			HashSet<String> bTags = stringUtils.getB(page);
			
			indexEntry.setbTags(bTags);
			indexEntry.setH1Tags(h1Tags);
			indexEntry.setH2Tags(h2Tags);
			indexEntry.setH3Tags(h3Tags);
			indexEntry.setMetaTags(metaTags);
			indexEntry.setTitle(title);
			indexEntry.setDocId(Integer.parseInt(records.get(url).get("_uid")));
			
			System.out.println("Record "+indexEntry.docId+"\n"+indexEntry.title+"\n"+indexEntry.metaTags+"\n"+indexEntry.bTags+"\n"+indexEntry.h1Tags+"\n"+indexEntry.h2Tags+"\n"+indexEntry.h3Tags);
		}
    	
    	dbwrapper.updateIndex(collection, indexEntry);
    }
    public static void crawlStats(){
    	MongoCollection<Document> collection = Constants.db.getCollection("webcrawler_data");
    	StringUtils stringUtils = new StringUtils();
    	MyCrawler crawled = new MyCrawler();
    	DBWrapper db = new DBWrapper();
    	int maxWordCount=0;
    	String maxWordCountURL="";
    	
    	for(int i=0;i<Constants.DB_ROW_COUNT;i++){
    		HashMap<String,HashMap<String,String>> records =db.fetchOne(collection,i); //don't make multiple db calls fetch records in batches of 500 and process
    		for(String url:records.keySet()){
    			crawled.addUniquePages(url);
    			crawled.findDomainsAndPages(stringUtils.getSubDomain(url), url);
    			stringUtils.tokenizePage(records.get(url).get("TEXT_RES"));
    			stringUtils.mapPageTo3Grams(records.get(url).get("TEXT_RES"));
    			
    			int wordcount = Integer.parseInt(records.get(url).get("NUM_WORDS"));
    			
    			if(wordcount>=maxWordCount){
    				maxWordCount=wordcount;
    				maxWordCountURL = url;
    			}
    		}
    	}
    	
    	System.out.println("*****Unique Pages******** "+Stats.uniquePages.size());
    	
    	File threeGramFile = new File("resources/Three_Grams.txt");
    	File subDomainsFile = new File("resources/Subdomains.txt");
    	File domainWordsFile = new File("resources/CommonDomainWords.txt");
    	File longestPage = new File("resources/longestPage.txt");
    	
    	try{
			if (!longestPage.exists()){
				longestPage.createNewFile();
			}	
			
			FileWriter fw = new FileWriter(longestPage);
			BufferedWriter bw = new BufferedWriter(fw);
			String content = "Longest Page url: "+maxWordCountURL +"\n"+"word count :"+maxWordCount;
			
			bw.write(content);
			bw.flush();
			bw.close();				
		} catch (Exception e){
			e.printStackTrace();
		}
    	
    	ArrayList<StringUtils.Pair> threeGrams = stringUtils.sortMap(Stats.threeGramSet);
    	ArrayList<StringUtils.Pair> commonWords = stringUtils.sortMap(Stats.tokenfrequencyList);
    	ArrayList<StringUtils.Pair> domainPgCount = stringUtils.sortSet(Stats.subDomainsPageCount);
    	
    	stringUtils.print(threeGrams,threeGramFile,20);
    	stringUtils.print(commonWords, domainWordsFile, 500);
    	stringUtils.print(domainPgCount, subDomainsFile, -1); // -1 => no limit

    }
    
    public static void generateInvertedIndex(HashMap<String,HashMap<String,String>> records, int docId, StringUtils stringUtils, DBWrapper dbwrapper){
    	HashMap<String,ArrayList<Integer>> termPositions=null;
    	HashMap<String, ArrayList<InvertedIndexEntry>> invertedIndex = new HashMap<String, ArrayList<InvertedIndexEntry>>();
    	
		for(String url:records.keySet()){
			ArrayList<String> terms = stringUtils.tokenizePage(records.get(url).get("TEXT_RES"));
			// positions of all terms in page
			termPositions = stringUtils.findTermPositions(terms);
			//records.get(url).get("HTML_RES");
		}
		//below processing per page
		for(String token:Stats.tokenfrequencyList.keySet()){
			ArrayList<InvertedIndexEntry> docItemList;
			if(invertedIndex.get(token)==null){
				docItemList = new ArrayList<InvertedIndexEntry>();
			} else {
				docItemList = invertedIndex.get(token);
			}
			
			InvertedIndexEntry docEntry = new InvertedIndexEntry();
			docEntry.setDocId(docId);
			docEntry.setTermFrequency(Stats.tokenfrequencyList.get(token));
			docEntry.setTermPositions(termPositions.get(token));
			docItemList.add(docEntry);
			invertedIndex.put(token,docItemList);        			
		}
		
		//flush token frequency as it is per page
		System.out.println(Stats.tokenfrequencyList.toString());
		Stats.tokenfrequencyList.clear();
		
		if((docId%100)==0){
			//write after every 100 records
			MongoCollection<Document> invertedIndexCollection = Constants.db.getCollection("inverted_index");
			dbwrapper.saveIndexBlock(invertedIndexCollection, invertedIndex);
			//flush
			invertedIndex.clear();        			
		}
    }
}