import java.util.HashSet;

public class IndexEntry {
	int docId;
	HashSet<String> title;
	HashSet<String> metaTags;
	HashSet<String> h1Tags;			
	HashSet<String> h2Tags;
	HashSet<String> h3Tags;
	HashSet<String> bTags;
	HashSet<Integer> indegreeDocIds;
	HashSet<String> aTags;
	
	public void setIndegreeDocIds(HashSet<Integer> indegreeDocIds) {
		this.indegreeDocIds = indegreeDocIds;
	}
	public void setaTags(HashSet<String> aTags) {
		this.aTags = aTags;
	}	
	public void setDocId(int docId) {
		this.docId = docId;
	}
	public void setTitle(HashSet<String> title) {
		this.title = title;
	}
	public void setMetaTags(HashSet<String> metaTags) {
		this.metaTags = metaTags;
	}
	public void setH1Tags(HashSet<String> h1Tags) {
		this.h1Tags = h1Tags;
	}
	public void setH2Tags(HashSet<String> h2Tags) {
		this.h2Tags = h2Tags;
	}
	public void setH3Tags(HashSet<String> h3Tags) {
		this.h3Tags = h3Tags;
	}
	public void setbTags(HashSet<String> bTags) {
		this.bTags = bTags;
	}	
}
