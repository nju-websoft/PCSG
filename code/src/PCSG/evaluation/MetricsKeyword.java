package PCSG.evaluation;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.DBUtil;
import PCSG.util.StemAnalyzer;

import java.nio.file.Paths;
import java.sql.Connection;
import java.util.*;

public class MetricsKeyword {
    Connection connection;
    private int dataset;

    public MetricsKeyword (int dataset){
        this.dataset = dataset;
        this.connection = new DBUtil().conn;
//        typeID = getTypeID(dataset);
    }

    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Map<String, Set<Triple>> kw2trp;
    Map<String, Set<Integer>>kw2ids;

    double cokNEW;
    public double coKyw(Set<Triple> snippet, List<String>kws){
        int count = 0;
        Set<Integer>ids = new HashSet<>();
        for(Triple iter: snippet) {
            ids.add(iter.getSid());
            ids.add(iter.getPid());
            ids.add(iter.getOid());
        }
        try {
            IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData +  "LabelIdIndex/" + dataset + "/"))));
            QueryParser parser = new QueryParser("label", new StemAnalyzer());
            kw2trp = new HashMap<>();
            kw2ids = new HashMap<>();
            for (String iter: kws){
                Set<Integer> hitSet = new HashSet<>();
                Query query = parser.parse(iter);
                TopDocs topDocs = indexSearcher.search(query, 100000000);
                if (topDocs != null){
                    for (ScoreDoc sdoc: topDocs.scoreDocs){
                        hitSet.add(Integer.parseInt(indexSearcher.doc(sdoc.doc).get("id")));
                    }
                }
                for (Integer id: ids){
                    if (hitSet.contains(id)){
                        count++;
                        break;
                    }
                }
                Set<Triple> tripleSet = new HashSet<>();
                for (Triple t: snippet){
                    if (hitSet.contains(t.getSid()) || hitSet.contains(t.getPid()) || hitSet.contains(t.getOid())){
                        tripleSet.add(t);
                    }
                }
                if (!tripleSet.isEmpty()) {
                    kw2trp.put(iter, tripleSet);
                }

                Set<Integer> temp = new HashSet<>();
                for (Integer id: ids){
                    if (hitSet.contains(id)){
                        temp.add(id);
                    }
                }
                kw2ids.put(iter, temp);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        cokNEW = ((double) count)/kws.size();
        return cokNEW;
    }

    public double coCnx(Set<Triple> snippet, List<String> kws){
        if(kws.size() == 1) {
            return cokNEW;
        }
        Map<Triple, Set<Triple>> neighbor = new HashMap<>(); //neighbor: triple -> its neighbors
        for(Triple iter: snippet) {
            Set<Triple>temp = new HashSet<>();
            int sid = iter.getSid();
            int oid = iter.getOid();
            for(Triple t: snippet) {
                if(t.getSid() == sid || t.getSid() == oid || t.getOid() == sid || t.getOid() == oid) {
                    temp.add(t);
                }
            }
            neighbor.put(iter, temp);
        }

        int count = 0;
        Set<Triple> checked;
        Set<Triple> nbs;
        for(int i = 0; i < kws.size() - 1; i++) {
            String start = kws.get(i);
            for(int j = i + 1; j < kws.size(); j++) {
                String end = kws.get(j);
                nbs = new HashSet<>();
                if(!kw2trp.containsKey(start) || !kw2trp.containsKey(end)) {
                    continue;
                }
                for(Triple iter0: kw2trp.get(start)){
                    nbs.addAll(neighbor.get(iter0));
                }
                checked = new HashSet<>();
                int flag = 0;
                while(!nbs.isEmpty()) {
                    Set<Triple>newnb = new HashSet<>();
                    Set<Triple>del = new HashSet<>();
                    for(Triple iter: nbs) {
                        if(kw2trp.get(end).contains(iter)) {
                            count ++;
                            flag = 1;
                            break;
                        }
                        else {
                            del.add(iter);
                            checked.add(iter);
                            for(Triple t: neighbor.get(iter)) {
                                if(!checked.contains(t) && !nbs.contains(t)) {
                                    newnb.add(t);
                                }
                            }
                        }
                    }
                    if(flag == 1) {
                        break;
                    }
                    nbs.addAll(newnb);
                    for(Triple d: del) {
                        nbs.remove(d);
                    }
                }
            }
        }
        return ((double)(2*count))/(kws.size()*(kws.size() - 1));
    }

}
