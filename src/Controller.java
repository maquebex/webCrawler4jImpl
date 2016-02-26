import java.util.ArrayList;
import java.util.HashMap;

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

        	/*
        	 * Instantiate the controller for this crawl.
        	 */
        	PageFetcher pageFetcher = new PageFetcher(config);
        	RobotstxtConfig robotstxtConfig = new RobotstxtConfig();
        	RobotstxtServer robotstxtServer = new RobotstxtServer(robotstxtConfig, pageFetcher);
        	CrawlController controller = new CrawlController(config, pageFetcher, robotstxtServer);

        	/*
        	 * For each crawl, you need to add some seed urls. These are the first
        	 * URLs that are fetched and then the crawler starts following links
        	 * which are found in these pages
        	 */
        	controller.addSeed("http://www.ics.uci.edu/");

        	/*
        	 * Start the crawl. This is a blocking operation, meaning that your code
        	 * will reach the line after this only when crawling is finished.
        	 */
        	controller.start(MyCrawler.class, numberOfCrawlers);
        	controller.shutdown();	// comment if needed
        	controller.waitUntilFinish();
        	Constants.mongoClient.close(); // comment if needed
        
        } else {
        	// process crawled data
        	StringUtils stringUtils = new StringUtils();
        	DBWrapper dbwrapper = new DBWrapper();
        	MongoCollection<Document> collection = Constants.db.getCollection("webcrawler_data");
        	HashMap<String, ArrayList<InvertedIndexEntry>> invertedIndex = new HashMap<String, ArrayList<InvertedIndexEntry>>();
        	
        	for(int i=1;i<=Constants.DB_ROW_COUNT;i++){
        		HashMap<String,HashMap<String,String>> records =dbwrapper.fetchOne(collection,i);
        		HashMap<String,ArrayList<Integer>> termPositions=null;
        		
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
        			docEntry.setDocId(i);
        			docEntry.setTermFrequency(Stats.tokenfrequencyList.get(token));
        			docEntry.setTermPositions(termPositions.get(token));
        			docItemList.add(docEntry);
        			invertedIndex.put(token,docItemList);        			
        		}
        		
        		//flush token frequency as it is per page
        		System.out.println(Stats.tokenfrequencyList.toString());
    			Stats.tokenfrequencyList.clear();
    			
        		if((i%100)==0){
        			//write after every 100 records
        			MongoCollection<Document> invertedIndexCollection = Constants.db.getCollection("inverted_index");
        			dbwrapper.saveIndexBlock(invertedIndexCollection, invertedIndex);
        			//flush
        			invertedIndex.clear();        			
        		}
        	}
        	/*StringUtils stringUtils = new StringUtils();
        	MyCrawler crawled = new MyCrawler();
        	DBWrapper db = new DBWrapper();
        	int maxWordCount=0;
        	String maxWordCountURL="";
        	
        	for(int i=0;i<Constants.DB_ROW_COUNT;i++){
        		HashMap<String,HashMap<String,String>> records =db.fetchOne(i); //don't make multiple db calls fetch records in batches of 500 and process
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
*/        }  
    }
}