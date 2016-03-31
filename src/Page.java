import java.util.Set;

public class Page {
	String Url;
	Set<String> outGoingLinks;
	double rank;
	int docId;
	
	public Page(String url, Set<String> outGoingLinks, int doc_id) {
		super();
		Url = url;
		this.outGoingLinks = outGoingLinks;
		this.rank = 0;
		this.docId = doc_id;
	}

	public String getUrl() {
		return Url;
	}

	public int getDocId() {
		return docId;
	}

	public void setDocId(int docId) {
		this.docId = docId;
	}

	public void setUrl(String url) {
		Url = url;
	}

	public Set<String> getOutGoingLinks() {
		return outGoingLinks;
	}

	public void setOutGoingLinks(Set<String> outGoingLinks) {
		this.outGoingLinks = outGoingLinks;
	}

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}
}
