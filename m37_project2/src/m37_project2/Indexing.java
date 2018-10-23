
package m37_project2;

import org.apache.lucene.index.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.*;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

import java.nio.file.Path;
import java.nio.file.Paths;

public class Indexing {

	

static HashMap<String, LinkedList<Integer>> invindex = new HashMap<String, LinkedList<Integer>>();
static int count = 0;
static int comparisons = 0;

public static void main(String[] args) throws Exception {
	Path p1 = Paths.get(args[0].toString());
	File file = new File(args[1].toString());
	Directory dir = FSDirectory.open(Paths.get(p1.toString()));
	DirectoryReader d = DirectoryReader.open(dir);
	IndexReader luceneIndexer = d;
	String queries;
	String[] query = null;
	BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(new File(args[2].toString())), "UTF-8"));
	 
	Indexing a = new Indexing(d, luceneIndexer);
	PrintWriter output = new PrintWriter(file);
	while ((queries = br.readLine()) != null) {

		query = queries.trim().split("\\s+");
		getPostings(query, output);
		taatAnd(query, output);
		taatOr(query, output);
		daatAnd(query,output);
		daatOr(query,output);
	}
	output.close();

	br.close();

}

public Indexing(DirectoryReader d, IndexReader luceneIndexer) throws IOException {

	String[] fields = { "text_en", "text_es", "text_fr" };

	for (String field : fields) {
		Terms terms = MultiFields.getTerms(d, field);

		TermsEnum t = terms.iterator();
		BytesRef term = t.next();
		while (term != null) {
			String termString = term.utf8ToString();
			PostingsEnum p = MultiFields.getTermDocsEnum(luceneIndexer, field, term);

			Integer docid = p.nextDoc();

			while (docid != PostingsEnum.NO_MORE_DOCS) {

				if (invindex.get(termString) == null) {
					LinkedList<Integer> temp = new LinkedList<Integer>();
					temp.add(docid);
					count++;
					invindex.put(termString, temp);
				} else {
					count++;
					invindex.get(termString).add(docid);
					Collections.sort(invindex.get(termString));
				}
				docid = p.nextDoc();
			}
			term = t.next();
		}
	}

	luceneIndexer.close();

}

public static void getPostings(String[] query, PrintWriter output) throws IOException {

	for (String term : query) {

		LinkedList<Integer> la = new LinkedList<Integer>();
		la = invindex.get(term);
		output.append("GetPostings");
output.append("\n");
output.append(term);
output.append("\n");
output.append("Postings list: ");
for(Integer item : la) {
	output.append(item+" ");
}
output.append("\n");
	}

}

public static void taatOr(String[] query, PrintWriter output) {

	LinkedList<Integer> partialResult = new LinkedList<Integer>();
	partialResult = null;
	if (query.length == 1) {
		for (String term : query)
			partialResult = invindex.get(term);
	}	

else {
	partialResult = invindex.get(query[0]);
	for (int i = 1; i < query.length; i++) {

		partialResult = compareOr(partialResult, invindex.get(query[i]));
	}

}
output.append("TaatOr");
output.append("\n");
for (int i = 0; i < query.length; i++) {
	output.append(query[i]);
	output.append(" ");
}
output.append("\n");
output.append("Results: ");
if(partialResult.isEmpty()) {
	output.append("empty");
}
else {
for(Integer item : partialResult) {


	output.append(item+" ");}
}
output.append("\n");
output.append("Number of documents in results: " + partialResult.size());
output.append("\n");
output.append("Number of comparisons: " + comparisons);
output.append("\n");
	comparisons = 0;

}

public static void taatAnd(String[] query, PrintWriter output) {

	LinkedList<Integer> partialResult = new LinkedList<Integer>();
	partialResult = null;
	if (query.length == 0) {
		// print Results: empty
} else if (query.length == 1) {
	for (String term : query)
		partialResult = invindex.get(term);
}

else {
	partialResult = invindex.get(query[0]);
	for (int i = 1; i < query.length; i++) {
		if (partialResult.size() == 0) {
			break;
		} else {
			LinkedList<Integer> temp = new LinkedList<Integer>();
			temp = invindex.get(query[i]);
			partialResult = compareAnd(partialResult, temp);
		}
	}

}
output.append("TaatAnd");
output.append("\n");
for (int i = 0; i < query.length; i++) {
	output.append(query[i]);
	output.append(" ");
}
output.append("\n");
output.append("Results: ");
if(partialResult.isEmpty()) {
	output.append("empty");
}
else {
for(Integer item : partialResult) {


	output.append(item+" ");}
}
output.append("\n");
output.append("Number of documents in results: " + partialResult.size());
output.append("\n");
output.append("Number of comparisons: " + comparisons);
output.append("\n");
	comparisons = 0;
}

public static LinkedList<Integer> compareOr(LinkedList<Integer> partialResult, LinkedList<Integer> term) {
	int i = 0;
	int j = 0;
	LinkedList<Integer> result = new LinkedList<Integer>();

	int sizea = partialResult.size();
	int sizeb = term.size();
	while (i < sizea && j < sizeb) {
		Integer t = (Integer)partialResult.get(i);
		Integer t1 = (Integer)term.get(j);
		if (t.compareTo(t1) == 0) {
			result.add(t);
			i++;
			j++;
			comparisons++;
		} else if (t.compareTo(t1) < 0) {
			result.add(t);
			i++;
			comparisons++;
		} else {
			result.add(t1);
			comparisons++;
			j++;
		}

	}
	if (i < sizea) {
		while (i < sizea) {
		Integer termz = partialResult.get(i);
		if(!result.contains(termz)) {
			result.add(termz);
		}
			i++;
	}
	}
	if (j < sizeb) {
		while (j < sizeb) {
			Integer termz = term.get(j);
			if(!result.contains(termz)) {
				result.add(termz);
			}
			j++;
		}
	}

	return result;

}
public static LinkedList<Integer> compareAnd(LinkedList<Integer> partialResult, LinkedList<Integer> term) {
	Integer i = 0;
	Integer j = 0;
	int jump1 = (int) (partialResult.size() / Math.sqrt(partialResult.size()));
	int jump2 = (int) (term.size() / Math.sqrt(term.size()));

	LinkedList<Integer> result = new LinkedList<Integer>();
	Iterator it = partialResult.iterator();
	Iterator it1 = term.iterator();
	int sizea = partialResult.size();
	int sizeb = term.size();
//	int blah = Math.min(sizea, sizeb);

Iter<Integer> a = new Iter<Integer>(partialResult);
Iter<Integer> b = new Iter<Integer>(term);

Integer t0 = (Integer) it.next();
Integer t1 = (Integer) it1.next();
while (t0!=null && t1!=null) {

	if (t0.compareTo(t1) == 0) {
		result.add(partialResult.get(i));
		comparisons++;
		if (!it.hasNext() || !it1.hasNext()) {
			break;
		}
		t0 = (int) it.next();
		t1 = (int) it1.next();
		// SKiplisting for partialResult LinkedList
} else if (t0.compareTo(t1) < 0) {
	comparisons++;
	if (a.getIndex(t0) != -1 && sizea - i >= jump1 && jump1 != 1) {

		if (a.hasNext()) {
			int k = i;
			t0 = a.next();
			if (t0.compareTo(t1) == 0) {
				comparisons++;
				i = i + jump1;
				for (int x = k; x <= i; x++) {
					it.next();
				}

				result.add(partialResult.get(i));
			} else if (t0.compareTo(t1) < 0) {
				comparisons++;
				i = i + jump1;
				for (int x = k; x < i; x++) {
					it.next();
				}

			} else {
				i = k;
				t0 = (int) it.next();
				i++;
				comparisons++;
			}
		}
		else {
			t0 = (int) it.next();
			i++;
		}
	} else {
		if (it.hasNext()) {
			t0 = (Integer) it.next();

		} else
			break;
		i++;
	}

}

// SkipListng for term LInkedList
		else {
			int k;
			comparisons++;
			if (j % jump2 == 0 && sizeb - j >= jump2 && jump2 != 1) {

				if (b.hasNext()) {
					k = j;
					t1 = b.next();

					if (t0.compareTo(t1) == 0) {
						comparisons++;
						j = j + jump2;
						for (int x = k; x <= j; x++) {
							it1.next();
						}

						result.add(term.get(j));
					} else if (t0.compareTo(t1) > 0) {
						comparisons++;
						j = j + jump2;
						for (int x = k; x <= j; x++) {
							t1 = (int) it1.next();
						}

					} else {
						j = k;
						t1 = (int) it1.next();
						j++;
						comparisons++;
					}

				}
				else {
					t1 = (int)it1.next();
					j++;
				}
			} else {
				if (it1.hasNext()) {
					t1 = (Integer) it1.next();
				} else
					break;
				j++;
			}

		}
	}
	return result;

}

public static void daatOr(String[] queries, PrintWriter output) throws IOException {
	PriorityQueue<Integer> pq = new PriorityQueue<Integer>();
	HashMap<Integer, ArrayList<Iterator<Integer>>> temp = new HashMap<Integer, ArrayList<Iterator<Integer>>>();
	LinkedList<Integer> result = new LinkedList<Integer>();
	for (String query : queries) {
		Iterator<Integer> it = invindex.get(query).iterator();
		if(it.hasNext()) {
			Integer docid = (Integer) it.next();
			if (temp.containsKey(docid)) {
				temp.get(docid).add(it);
				pq.add(docid);
				
			} else {
				ArrayList<Iterator<Integer>> n = new ArrayList<Iterator<Integer>>();
				n.add(it);
				temp.put(docid, n);
				pq.add(docid);
			}
			
			
		}

	}
	while (!pq.isEmpty()) {
		if(pq.size() == 1) {
			Integer t = pq.poll();
			comparisons++;
			if(!result.contains(t)) {
				result.add(t);
			}
		}
		else {
			Integer t = pq.poll();
			comparisons++;

			if(!result.contains(t)) {
				
				result.add(t);
			}

			ArrayList<Iterator<Integer>> n = new ArrayList<Iterator<Integer>>();
			n = temp.get(t);
			for (Iterator<Integer> its : n) {
	
				if (its.hasNext()) {
					Integer tempdoc = (Integer)its.next();
					pq.add(tempdoc);
		
					if (temp.containsKey(tempdoc)) {
						temp.get(tempdoc).add(its);
			
			
					} else {
						ArrayList<Iterator<Integer>> n1 = new ArrayList<Iterator<Integer>>();
						n1.add(its);
						temp.put(tempdoc, n1);
			
					}
		
		
				}

			}
		}

	}
output.append("DaatOr");
output.append("\n");
for (int i = 0; i < queries.length; i++) {
	output.append(queries[i]);
	output.append(" ");
}
output.append("\n");
output.append("Results: ");
if(result.isEmpty()) {
	output.append("empty");
}
else {
for(Integer item : result) {
	output.append(item+" ");}
}
output.append("\n");
output.append("Number of documents in results: " + result.size());
output.append("\n");
output.append("Number of comparisons: " + comparisons);
output.append("\n");
comparisons = 0;
}
public static void daatAnd(String[] queries, PrintWriter output) {
	PriorityQueue<Integer> pq = new PriorityQueue<Integer>();
	LinkedList<Integer> result = new LinkedList<Integer>();
	HashMap<Integer,ArrayList<Iter<Integer>>> temp = new HashMap<Integer,ArrayList<Iter<Integer>>>();
	
	for(String query: queries) {
		LinkedList<Integer> docids = invindex.get(query);
		Iter<Integer> itr = new Iter<Integer>(docids);
		Integer docid;
		
		if(!docids.isEmpty()) {
			docid = (Integer) itr.plusOne();
			
			if (temp.containsKey(docid)) {
				temp.get(docid).add(itr);
				pq.add(docid);
				
			} 
			else {
				ArrayList<Iter<Integer>> n = new ArrayList<Iter<Integer>>();
				n.add(itr);
				temp.put(docid, n);
				pq.add(docid);
			}
			
			
		}
		else {
			return;
		}
	}
meraouterloop:	while (!pq.isEmpty()) {
		
		if(pq.size() == 1) {
			Integer t = pq.poll();
			if(!result.contains(t)) {
				result.add(t);
			}
		}
		else {
			boolean flag = true;
			Integer t = pq.poll();
			comparisons++;
			merafor: for(Integer elem : pq) {
				if(t.compareTo(elem) != 0) {
					
					flag = false;
					break merafor;	
				}
			}
			if(flag) {
				pq.clear();
				result.add(t);
				for(Iter<Integer> ab : temp.get(t)) {
					ab.incCurr();
					if(ab.hasNext()) {
						Integer x = (Integer)ab.plusOne();
						if (temp.get(x) == null) {
							ArrayList<Iter<Integer>> n = new ArrayList<Iter<Integer>>();
							n.add(ab);
							temp.put(x, n);
						} else {
							ArrayList<Iter<Integer>> n = temp.get(x);
							n.add(ab);
							temp.put(x, n);
						}
						pq.add((Integer)ab.plusOne());
					}
					else {
						writer(output, comparisons, queries, result);
						comparisons = 0;
						return;
					}
				}

			}
			else {
				
				ArrayList<Iter<Integer>> x = temp.get(t);
				Iter<Integer> it = x.remove(0);
					if(it.hasNext()) {
						if(it.getIndex(t) != -1 && it.check()) {
							Iter<Integer> it1 = it;
							int prev = it1.index(it1.plusOne());
							
							Integer doc = it.next();
							int index = it.index(doc);
							comparisons++;
							if(doc.compareTo(pq.peek()) < 0) {
								pq.add(doc);
								ArrayList<Iter<Integer>> n;
								if (temp.get(doc) != null) {
									n = temp.get(doc);
								} else 
									n = new ArrayList<Iter<Integer>>();
									
								n.add(it);
								temp.put(doc, n);
							}
							else {
								for(int k = prev; k<index - 1;k++) {
									it.decCurr();
								}
								doc = it.plusOne();
								ArrayList<Iter<Integer>> n;
								if (temp.get(doc) != null) {
									n = temp.get(doc);
								} else 
									n = new ArrayList<Iter<Integer>>();
									
								n.add(it);
								temp.put(doc, n);
								pq.add(it.plusOne());
								
							}
						}
						else {
							it.incCurr();
							int doc = it.plusOne();
							ArrayList<Iter<Integer>> n;
							if (temp.get(doc) != null) {
								n = temp.get(doc);
							} else 
								n = new ArrayList<Iter<Integer>>();
								
							n.add(it);
							temp.put(doc, n);
							pq.add(it.plusOne());
						}
					}
					//if it does not has next then
					else if(it.normalNext()) {
						it.incCurr();
						int doc = it.plusOne();
						ArrayList<Iter<Integer>> n;
						if (temp.get(doc) != null) {
							n = temp.get(doc);
						} else 
							n = new ArrayList<Iter<Integer>>();
							
						n.add(it);
						temp.put(doc, n);
						pq.add(it.plusOne());
					}
					else {
						writer(output, comparisons, queries, result);
						comparisons = 0;
						return;
					}
				}
			}
		}
	}
	


public static void writer(PrintWriter output, int comparisons, String[] queries, LinkedList<Integer>result) {
output.append("DaatAnd");
output.append("\n");
for (int i = 0; i < queries.length; i++) {
	output.append(queries[i]);
	output.append(" ");
}
output.append("\n");
output.append("Results: ");
if(result.isEmpty()) {
	output.append("empty");
}
else {
for(Integer item : result) {


	output.append(item+" ");}
}
output.append("\n");
output.append("Number of documents in results: " + result.size());
output.append("\n");
output.append("Number of comparisons: " + comparisons);
output.append("\n");
}
}

class Iter<Integer> implements Iterator<Integer> {

	private LinkedList<Integer> ll = new LinkedList<Integer>();
	private int size;
	private int curr;
	private int freq;

	public Iter(LinkedList<Integer> l) {
		this.ll = l;
		this.curr = 0;
		this.size = l.size();
		this.freq = (int) (size / Math.sqrt(size));
	}

	public Iter(LinkedList<Integer> l, int i) {
		this.ll = l;
		curr = i;
		this.size = l.size();
		this.freq = (int) (size / Math.sqrt(size));
	}

	@Override
	public boolean hasNext() {
		return curr+freq <= size-1;
	}
	public boolean normalNext() {
		return curr < size - 1;
	}
	public boolean check() {
		return curr+freq < size;
	}

	@Override
	public Integer next() {

		Integer element = ll.get(curr + freq);
		this.curr = curr + freq;

		return element;

	}
	public int prevIndex(Integer docid) {
		int id = ll.indexOf(docid);
		return id;
	}
	public Integer firstNext() {
		Integer element = ll.get(curr);
		curr--;
		return element;
	}
	public void incCurr() {
		this.curr++;
	}
	public Integer plusOne() {
		Integer elem = ll.get(curr);
		return elem;
	}
	public void decCurr() {
		this.curr--;
	}
	public int getIndex(Integer docid) {
		int id = ll.indexOf(docid);
		if(id%freq == 0) {
			return id;
		}
		else {
			return -1;
		}
	}
	public int index(Integer docid) {
		return ll.indexOf(docid);
	}
}
