import java.sql.Timestamp;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

import edu.uci.ics.crawler4j.crawler.Page;
import edu.uci.ics.crawler4j.crawler.WebCrawler;
import edu.uci.ics.crawler4j.parser.HtmlParseData;
import edu.uci.ics.crawler4j.url.WebURL;

public class MyCrawler extends WebCrawler {
	
	java.util.Date date= new java.util.Date();
	ExecutorService executorService = Executors.newFixedThreadPool(Constants.THREAD_POOL_SIZE);
	private final static Pattern FILTERS = Pattern.compile(".*(\\.(css|js|bmp|gif|jpe?g|png|"+
																"tiff|mid|mp2|mp3|mp4|zip|" +
																"wav|avi|mov|mpeg|ram|m4v|gz|db|iso|tar|gz|apk|ba|" +
																"rm|smil|wmv|swf|wma|rar|ps|ppt|pptx|doc|docx|xlsx|pdf|\\?))$");

    /**
     * This method receives two parameters. The first parameter is the page
     * in which we have discovered this new url and the second parameter is
     * the new url. You should implement this function to specify whether
     * the given url should be crawled or not (based on your crawling logic).
     * In this example, we are instructing the crawler to ignore urls that
     * have css, js, git, ... extensions and to only accept urls that start
     * with "http://www.ics.uci.edu/". In this case, we didn't need the
     * referringPage parameter to make the decision.
     */
     @Override
     public boolean shouldVisit(Page referringPage, WebURL url) {
    	 addUniquePages(url);
    	 String href = url.getURL().toLowerCase();
         return !FILTERS.matcher(href).matches()
                && href.contains(Constants.DOMAIN)&& href.indexOf('?')==-1&& !href.contains("mailto") && !href.contains("duttgroup") && !href.contains("contact/student-affairs/contact/student-affairs");
     }

     /**
      * This function is called when a page is fetched and ready
      * to be processed by your program.
      */
     @Override
     public void visit(Page page) {
         /*String url = page.getWebURL().getURL();
         String domain = page.getWebURL().getDomain();
         String path = page.getWebURL().getPath();
         String subDomain = page.getWebURL().getSubDomain();
         
         logger.info("URL: {}", url);
         logger.debug("Domain: '{}'", domain);
         logger.info("Sub-domain: '{}'", subDomain);
         logger.debug("Path: '{}'", path);*/
         
         //findDomainsAndPages(subDomain, url);   need to uncomment this later      
         parseResponse(page);
    }
     
    public void addUniquePages(WebURL url){
    	Stats.uniquePages.add(url);
    }
    
    public void findDomainsAndPages(String subDomain, String url){
    	if(Stats.subDomains.get(subDomain)!=null){
    		Stats.subDomains.get(subDomain).add(url);        	 
        } else {
       	 	HashSet<String> urlList = new HashSet<String>();
       	 	urlList.add(url);
       	 	Stats.subDomains.put(subDomain, urlList);
        }
        
        setDomainPageCount(subDomain);       
    }
    
    public void setDomainPageCount(String subDomain){
    	Stats.subDomainsPageCount.put(subDomain, Stats.subDomains.get(subDomain).size());
    	System.out.println(Stats.subDomainsPageCount.toString());
    }
    
    public void parseResponse(Page page){
    	if (page.getParseData() instanceof HtmlParseData) {
            HtmlParseData htmlParseData = (HtmlParseData) page.getParseData();
            String text = htmlParseData.getText();
            String html = htmlParseData.getHtml();
            Set<WebURL> links = htmlParseData.getOutgoingUrls();
            Set<String> outgoingLinks = new HashSet<String>();
            
            for(WebURL wurl:links){
            	outgoingLinks.add(wurl.toString());
            }
            
            HashMap<String, Object> responseBody = new HashMap<String,Object>();
            responseBody.put("URL", page.getWebURL().getURL());
            responseBody.put("HTML_RES", html);
            responseBody.put("TEXT_RES", text);
            responseBody.put("OUTGOING_LINKS", outgoingLinks);
            responseBody.put("DOMAIN",page.getWebURL().getDomain());
            responseBody.put("PATH",page.getWebURL().getPath());
            responseBody.put("SUBDOMAIN",page.getWebURL().getSubDomain().substring(4));
            responseBody.put("NUM_WORDS", new StringUtils().splitToWords(text).length);
            
            logger.debug("URL: {}", page.getWebURL().getURL());
            logger.debug("Sub-domain: '{}'", page.getWebURL().getSubDomain().substring(4));
            logger.debug("Time stamp "+ new Timestamp(date.getTime()));
            System.out.println("Time stamp "+new Timestamp(date.getTime())+" "+page.getWebURL().getURL());
            
            executorService.submit(new MongoDBConnector("webcrawler_data","counters_collection",responseBody)); 
            
            //System.out.println(responseBody.toString());            
            //conn.insertDocument("webcrawler_data",responseBody);
            /*System.out.println("Text url: " + page.getWebURL().getURL());
            System.out.println("Text length: " + text.length());
            System.out.println("Html length: " + html.length());
            System.out.println("Number of outgoing links: " + links.size());*/
            
        }
    }
}