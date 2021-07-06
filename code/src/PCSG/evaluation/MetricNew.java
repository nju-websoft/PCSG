package PCSG.evaluation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.DBUtil;
import PCSG.util.ReadFile;
import PCSG.util.StemAnalyzer;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class MetricNew {
    Connection connection;
    private int typeID; //triple whose uri = 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type', 0: not exist.
    private int dataset;
    Set<Integer> entityWithEDP; // set of entities with complete EDP, initiated in edpCover, used in lpCover

    public MetricNew(int dataset){
        this.dataset = dataset;
        this.connection = new DBUtil().conn;
        typeID = getTypeID(dataset);
    }

    public void close() {
        try {
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public int getTypeID(int dataset){
        String select = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
        try {
            PreparedStatement selectStatement = connection.prepareStatement(select);
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next();
            typeID = resultSet.getInt("type_id");
        }catch (Exception e){
            e.printStackTrace();
        }
        return typeID;
    }

    public double kwsCover(Set<Triple> snippet, List<String> kws){ // used in KSD and KeyKG1?
        int count = 0;
        Set<Integer>ids = new HashSet<>();
        for(Triple iter: snippet) { //get all IDs from the snippet
            ids.add(iter.getSid());
            ids.add(iter.getPid());
            ids.add(iter.getOid());
        }
        try {
            IndexSearcher indexSearcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData +  "LabelIdIndex/" + dataset + "/"))));
            QueryParser parser = new QueryParser("label", new StemAnalyzer());
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
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return ((double)count)/kws.size();
    }

    public double ClassCover(Set<Triple> snippet) {
        if (typeID == 0) {
            return -1;
        }
        Map<Integer, Double> class2Freq = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "FrequencyOfClass/" + dataset + ".txt", "\t")) {
            class2Freq.put(Integer.parseInt(iter.get(0)), Double.parseDouble(iter.get(2)));
        }
        if (snippet.size() == 1) {
            for (Triple t: snippet) {
                if (t.getPid() == 0) {
                    if (class2Freq.containsKey(t.getSid())) {
                        return class2Freq.get(t.getSid());
                    }
                    else return 0;
                }
            }
        }
        double result = 0;
        Set<Integer> classSet = new HashSet<>();
        for (Triple t: snippet) {
            if (t.getPid() == typeID && !classSet.contains(t.getOid())) {
                result += class2Freq.get(t.getOid());
                classSet.add(t.getOid());
            }
        }
        return result;
    }

    public double PropCover(Set<Triple> snippet) {
        Map<Integer, Double> prop2Freq = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "FrequencyOfProperty/" + dataset + ".txt", "\t")) {
            prop2Freq.put(Integer.parseInt(iter.get(0)), Double.parseDouble(iter.get(2)));
        }
        if (snippet.size() == 1) {
            for (Triple t: snippet) {
                if (t.getPid() == 0) {
                    return 0;
                }
            }
        }
        double result = 0;
        Set<Integer> propSet = new HashSet<>();
        for (Triple t: snippet) {
            if (!propSet.contains(t.getPid())) {
                result += prop2Freq.get(t.getPid());
                propSet.add(t.getPid());
            }
        }
        return result;
    }

    public double edpCover(Set<Triple> snippet) {
        entityWithEDP = new HashSet<>();
        if (snippet.size() == 1) {
            for (Triple t: snippet) {
                if (t.getPid() == 0) {
                    return 0;
                }
            }
        }
        Map<Integer, Set<Integer>> edpId2pattern = new HashMap<>();
        Map<Integer, Integer> edpId2count = new HashMap<>();
        Map<Integer, Integer> entity2edpId = new HashMap<>();
        try {
            IndexReader edpReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "EDPIndex/" + dataset + "/")));
            for (int i = 0; i < edpReader.maxDoc(); i++) {
                Document doc = edpReader.document(i);
                Set<Integer> pattern = new HashSet<>();
                String cStr = doc.get("classes");
                if (cStr.length() > 0) {
                    for (String c: cStr.split(" ")) {
                        pattern.add(Integer.parseInt(c));
                    }
                }
                String ipStr = doc.get("inProperty");
                if (ipStr.length() > 0) {
                    for (String ip: ipStr.split(" ")) {
                        pattern.add(-Integer.parseInt(ip));
                    }
                }
                String opStr = doc.get("outProperty");
                if (opStr.length() > 0) {
                    for (String op: opStr.split(" ")) {
                        pattern.add(Integer.parseInt(op));
                    }
                }
                edpId2pattern.put(Integer.parseInt(doc.get("id")), pattern);
            }
            edpReader.close();
            IndexReader entityReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "Entity2EDP/" + dataset + "/")));
            for (int i = 0; i < entityReader.maxDoc(); i++) {
                Document doc = entityReader.document(i);
                int edpId = Integer.parseInt(doc.get("edpId"));
                entity2edpId.put(Integer.parseInt(doc.get("entity")), edpId);
                edpId2count.put(edpId, Integer.parseInt(doc.get("count")));
            }
            entityReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        Map<Integer, Set<Integer>> snipPattern = new HashMap<>();
        for (Triple t: snippet) {
            int s = t.getSid();
            int p = t.getPid();
            int o = t.getOid();
            Set<Integer> pattern = snipPattern.getOrDefault(s, new HashSet<>());
            if (typeID != 0 && p == typeID) {
                pattern.add(o);
            }
            else {
                pattern.add(p);
            }
            snipPattern.put(s, pattern);
            if ((typeID == 0 || p != typeID) && entity2edpId.containsKey(o)) {
                pattern = snipPattern.getOrDefault(o, new HashSet<>());
                pattern.add(-p);
                snipPattern.put(o, pattern);
            }
        }
        int count = 0;
        Set<Integer> coveredEDPid = new HashSet<>();
        for (Map.Entry<Integer, Set<Integer>> iter: snipPattern.entrySet()) {
            int entity = iter.getKey();
            Set<Integer> pattern = iter.getValue();
            int edpId = entity2edpId.get(entity);
            Set<Integer> oracle = edpId2pattern.get(edpId);
            if (pattern.equals(oracle)) {
                entityWithEDP.add(entity);
                if (!coveredEDPid.contains(edpId)) {
                    coveredEDPid.add(edpId);
                    count += edpId2count.get(edpId);
                }
            }
        }
        return ((double) count)/((double) entity2edpId.size());
    }

    public double lpCover(Set<Triple> snippet){
        if (entityWithEDP.isEmpty()) {
            return 0;
        }
        int amount = 0;
        Map<String, String> triple2lp = new HashMap<>();
        Map<String, Integer> lp2count = new HashMap<>();
        try {
            IndexReader lpReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "Triple2LP/" + dataset + "/")));
            amount = lpReader.maxDoc();
            for (int i = 0; i < lpReader.maxDoc(); i++) {
                Document doc = lpReader.document(i);
                triple2lp.put(doc.get("triple"), doc.get("lp"));
                lp2count.put(doc.get("lp"), Integer.parseInt(doc.get("count")));
            }
            lpReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int count = 0;
        Set<String> coveredLP = new HashSet<>();
        for (Triple t: snippet) {
            int s = t.getSid();
            int p = t.getPid();
            int o = t.getOid();
            if (!entityWithEDP.contains(s) || !entityWithEDP.contains(o)) {
                continue;
            }
            String triple = s + " " + p + " " + o;
            if (triple2lp.containsKey(triple) && !coveredLP.contains(triple2lp.get(triple))) {
                count += lp2count.get(triple2lp.get(triple));
                coveredLP.add(triple2lp.get(triple));
            }
        }
        return ((double) count)/((double) amount);
    }

}
