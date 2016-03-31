import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;

import org.jsoup.Jsoup;

public class StringUtils {
	
	public StringUtils(){
		loadStopWords();		
	} 
	
	public HashSet<String> getTitleTokens(String page){
		String titleString="";
		HashSet<String> titleTokens = new HashSet<String>();
		try{
			titleString= Jsoup.parse(page).title();//page.substring(page.indexOf("<title>")+7,page.indexOf("</title>"));
		
		} catch (Exception e){
			System.out.println(e.getMessage());
		}
		
		if(titleString.length()!=0){
			ArrayList<String> list =tokenizePage(titleString);			
			titleTokens.addAll(list);
		}
		
		return titleTokens;
	}
	
	public HashSet<String> getH1(String page){
		int h1Index=0;
		HashSet<String> h1Tags = new HashSet<String>();
		
		while(h1Index<page.length()){
			try{
				int startIndex = page.indexOf("<h1",h1Index);
				int endIndex = page.indexOf("</h1>",startIndex);
				String h1String = page.substring(startIndex,endIndex);
				
				if(!h1String.contains("class=\"ahem\"")){
					h1String = Jsoup.parse(h1String).text();//page.substring(page.indexOf(">",startIndex)+1,endIndex);
					ArrayList<String> h1list = tokenizePage(h1String);
					h1Tags.addAll(h1list);
				}
				h1Index = startIndex + 4;				
				
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting h1 tag "+e.getMessage());
				h1Index = page.length();
			}
		}
		
		return h1Tags;
	}
	
	public HashSet<String> getH2(String page){
		int h2Index=0;
		HashSet<String> h2Tags = new HashSet<String>();
		
		while(h2Index<page.length()){
			try{
				int startIndex = page.indexOf("<h2",h2Index);
				int endIndex = page.indexOf("</h2>",startIndex);
				String h2String = page.substring(startIndex,endIndex);
				
				if(!h2String.contains("class=\"ahem\"")){
					h2String = Jsoup.parse(h2String).text();//page.substring(page.indexOf(">",startIndex)+1,endIndex);
					ArrayList<String> h2list = tokenizePage(h2String);
					h2Tags.addAll(h2list);
				}
				h2Index = startIndex + 4;				
				
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting h2 tag "+e.getMessage());
				h2Index = page.length();
			}
		}
		
		return h2Tags;
		
	}
	
	public HashSet<String> getH3(String page){
		int h3Index=0;
		HashSet<String> h3Tags = new HashSet<String>();
		
		while(h3Index < page.length()){
			try{
				int startIndex = page.indexOf("<h3",h3Index);
				int endIndex = page.indexOf("</h3>",startIndex);
				String h3String = page.substring(startIndex,endIndex);
				
				if(!h3String.contains("class=\"ahem\"")){
					h3String = Jsoup.parse(h3String).text();//page.substring(page.indexOf(">",startIndex)+1,endIndex);
					ArrayList<String> h3list = tokenizePage(h3String);
					h3Tags.addAll(h3list);
				}
				h3Index = startIndex + 4;				
				
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting h3 tag "+e.getMessage());
				h3Index = page.length();
			}
		}
		
		return h3Tags;
		
	}
	
	public HashSet<String> getB(String page){
		int bIndex=0;
		HashSet<String> bTags = new HashSet<String>();
		
		while(bIndex<page.length()){
			try{
				int startIndex = page.indexOf("<b>",bIndex);
				int endIndex = page.indexOf("</b>",startIndex);
				String bString = page.substring(startIndex,endIndex);
				bString = Jsoup.parse(bString).text();
				ArrayList<String> blist = tokenizePage(bString);
				bTags.addAll(blist);
				bIndex = startIndex + 3;				
				
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting b tag "+e.getMessage());
				bIndex = page.length();
			}
		}
		
		return bTags;
		
	}
	
	public HashSet<String> getAnchorData(String page){
		int aIndex=0;
		HashSet<String> aTags = new HashSet<String>();
		
		while(aIndex<page.length()){
			try{
				int startIndex = page.indexOf("<a",aIndex);
				int endIndex = page.indexOf("</a>",startIndex);
				String aString = page.substring(startIndex,endIndex);
				aString = Jsoup.parse(aString).text();
				ArrayList<String> alist = tokenizePage(aString);
				aTags.addAll(alist);
				aIndex = startIndex + 2;				
				
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting a tag "+e.getMessage());
				aIndex = page.length();
			}
		}
		System.out.println("a tags "+aTags.toString());
		return aTags;
	}
	
	public HashSet<String> getMetaData(String page){
		int metaIndex=0;
		HashSet<String> metaTags = new HashSet<String>();
		
		while(metaIndex<page.length()){
			try{
				int startIndex = page.indexOf("<meta",metaIndex);
				int endIndex = page.indexOf(">",startIndex);
				
				String metaString = page.substring(startIndex,endIndex+1);
				//System.out.println("Meta String "+metaString);
				
				if(metaString.contains("name")){
					int st = metaString.indexOf("name=")+6;
					int end = metaString.indexOf("\"",st);
					
					String metaType = metaString.substring(st,end);
					//System.out.println("Meta Type "+metaType);
					
					if(metaType.equals("keywords")|| metaType.equals("description")){
						int s = metaString.indexOf("content=")+9;
						int e = metaString.indexOf("\"",s);
						String metaTag = metaString.substring(s,e);
						ArrayList<String> metaTokens = tokenizePage(metaTag);
						metaTags.addAll(metaTokens);
					}
				}
				metaIndex = startIndex + 5;
			
			} catch( StringIndexOutOfBoundsException e){
				System.out.println("Error getting meta tag "+e.getMessage());
				metaIndex=page.length();
			}
		}
		
		return metaTags;
	} 
	public ArrayList<String> tokenizePage(String page){
		ArrayList<String> tokensForPage = new ArrayList<String>(); // should not be a set. need to generate separate token list for each page for 3-grams
		if(page.length()==0){
			return tokensForPage;
		}
		
		BufferedReader bf=new BufferedReader(new StringReader(page));		
		String line;
		try{
			while((line=bf.readLine())!=null){
				line = line.replaceAll("[.,;]", " ");
				String [] words = line.toLowerCase().split(" ");

				for(int i=0;i<words.length;i++){
					words[i]=words[i].replaceAll("\\n+", " ");
					words[i]=words[i].replaceAll("[^a-z0-9]+", "");
					words[i]=words[i].toLowerCase().trim();
					
					//for term positions
					if(!(words[i].matches("\\n+")||words[i].matches("\\s+")|| words[i].length()<2)){
						//tokensForPage.add(words[i]);					
						if(!(Stats.stopWords.contains(words[i]))){
							tokensForPage.add(words[i]);
							//addToFrequencyList(words[i]);
						}	
					}	
				}
			}
			
		} catch (Exception e){
			e.printStackTrace();
		}
		
		return tokensForPage;
	}
	public String[] splitToWords(String page){
		//stop words considered as words
		return page.split("[.,;\\s\\n]");		
		
	}
	
	public HashMap<String,ArrayList<Integer>> findTermPositions(ArrayList<String> terms){
		HashMap<String,ArrayList<Integer>> termPositions = new HashMap<String,ArrayList<Integer>>(); 
		for(int i=0;i<terms.size();i++){
			ArrayList<Integer> positions;
			if(termPositions.get(terms.get(i))==null){
				positions = new ArrayList<Integer>();
			} else {
				positions = termPositions.get(terms.get(i));
			}
			
			positions.add(i);
			termPositions.put(terms.get(i), positions);
		}
		
		return termPositions;
	}
	
	public String getSubDomain(String url){
		String subdomain="";
		try{
			if(url.contains("www")){
				subdomain=url.substring(url.indexOf("www.")+4,url.indexOf(".ics"));
			} else {
				subdomain = url.substring(url.indexOf("//")+2,url.indexOf(".ics"));
			}
		} catch (StringIndexOutOfBoundsException e){
			
		}
		System.out.println("sub "+subdomain +"  "+url);
		return subdomain;
	}
	public void loadStopWords(){		
		try{
			Scanner in = new Scanner(new File(Constants.FILE_NAME));
			while(in.hasNext()){
				String current_word = in.next();
				Stats.stopWords.add(current_word);
			}

			in.close();

		} catch (FileNotFoundException e){
			e.printStackTrace();

		}
	}

	public void addToFrequencyList(String current_word){
		if(!Stats.tokenfrequencyList.containsKey(current_word))
			Stats.tokenfrequencyList.put(current_word, 1);
		else{
			Stats.tokenfrequencyList.put(current_word, Stats.tokenfrequencyList.get(current_word)+1);
		}
	}

	public void addToUrlWordList(String current_word, String url){
		if(Stats.urlWordList.containsKey(current_word)){
			Stats.urlWordList.get(current_word).add(url);

		} else{
			HashSet<String> urlList = new HashSet<String>();
			urlList.add(url);
			Stats.urlWordList.put(current_word, urlList);
		}	
	}

	public void mapWordsToURL(String page, String url){
		tokenizePage(page);
		for(String current_word: Stats.tokenfrequencyList.keySet()){
			addToUrlWordList(current_word, url);
		}		
	}
	
	public void mapPageTo3Grams(String page){
		String threeGram = "";
		ArrayList<String> tokens = tokenizePage(page);
		
		for(int i=0;i<tokens.size()-3;i++){
			threeGram = tokens.get(i)+" "+tokens.get(i+1)+" "+tokens.get(i+2);
			threeGram= threeGram.trim();
			if(Stats.threeGramSet.get(threeGram)==null){
				Stats.threeGramSet.put(threeGram, 1);
			} else {
				Stats.threeGramSet.put(threeGram, Stats.threeGramSet.get(threeGram)+1);
			}
		}		
	}
	
	public ArrayList<Pair> sortMap(Map<String, Integer> freq){
		ArrayList<String> list = new ArrayList<String>(freq.keySet());
		Comparator<String> cmp = new Comparator<String>() {
			@Override
			public int compare(String s1, String s2) {
				Integer one = freq.get(s1);
				Integer two = freq.get(s2);
				return one.compareTo(two);
			}
		};
	
		Collections.sort(list, Collections.reverseOrder(cmp));
		ArrayList<Pair> listPair = new ArrayList<Pair>();
		for(String w: list){
			Pair p = new Pair();
			p.word = new String(w);
			p.frequency = freq.get(w);
			listPair.add(p);
		}
		return listPair;
	}
	
	public ArrayList<Pair> sortSet(Map<String, Integer> subDomainsPageCount){
		ArrayList<String> list = new ArrayList<String>(subDomainsPageCount.keySet());
		Comparator<String> cmp = new Comparator<String>(){
			@Override
			public int compare(String s1, String s2) {
				return s1.compareTo(s2);
			}
		};
		Collections.sort(list);
		ArrayList<Pair> listPair = new ArrayList<Pair>();
		for(String w: list){
			Pair p = new Pair();
			p.word = new String(w);
			p.frequency = subDomainsPageCount.get(w);
			listPair.add(p);
		}
		return listPair;
	}
	
	public void print(ArrayList<Pair> pairs, File fileToPrint, int limit){
		File file = fileToPrint;
		int i=0;
		try{
			if (!file.exists()){
				file.createNewFile();
			}	
			FileWriter fw = new FileWriter(file);
			BufferedWriter bw = new BufferedWriter(fw);
			for(Pair p: pairs){
				String content = p.word.toString() + ":" + p.frequency;
				bw.write(content);
				bw.newLine();
				if(limit !=-1){	// limit -1 == no limit
					i++;
					if(i==limit)
						break;
				}
			}
			bw.flush();
			bw.close();				
		} catch (Exception e){
			e.printStackTrace();
		}		
	}
	
	class Pair{
		public String word;
		public int frequency;
	}
}
