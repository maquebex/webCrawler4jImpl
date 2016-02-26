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

public class StringUtils {

	public ArrayList<String> tokenizePage(String page){
		loadStopWords();
		ArrayList<String> tokensForPage = new ArrayList<String>(); // should not be a set. need to generate separate token list for each page for 3-grams
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
						tokensForPage.add(words[i]);					
						if(!(Stats.stopWords.contains(words[i]))){
							addToFrequencyList(words[i]);
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
