package PCSG.SnippetGeneration;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Multigraph;
import PCSG.PATHS;
import PCSG.util.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class DatasetIndexer {

    /** Used in preprocess, part-1
     * @DATE: 2020/11
     * */
    private static boolean generatePatternIndex(int dataset) {
        Connection connection = new DBUtil().conn;
        String getType = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
        String getLiteral = "select id from uri_label_id? where is_literal = 1 and dataset_local_id = ?";
        String select = "select subject, predicate, object from triple? where dataset_local_id = ?";
        try {
            PreparedStatement getTypeStatement = connection.prepareStatement(getType);
            PreparedStatement getLiteralStatement = connection.prepareStatement(getLiteral);
            PreparedStatement selectStatement = connection.prepareStatement(select);
            ResultSet resultSet = getTypeStatement.executeQuery();
            resultSet.next();
            int typeID = resultSet.getInt("type_id");
            if (dataset <= 311){
                getLiteralStatement.setInt(1, 2);
                getLiteralStatement.setInt(2, dataset);
                selectStatement.setInt(1, 2);
                selectStatement.setInt(2, dataset);
            }
            else {
                getLiteralStatement.setInt(1, 3);
                getLiteralStatement.setInt(2, (dataset - 311));
                selectStatement.setInt(1, 3);
                selectStatement.setInt(2, (dataset - 311));
            }
            HashSet<Integer> literalSet = new HashSet<>();
            resultSet = getLiteralStatement.executeQuery();
            while(resultSet.next()) {
                literalSet.add(resultSet.getInt("id"));
            }

            resultSet = selectStatement.executeQuery();
            HashMap<Integer, HashSet<Integer>> id2EDP = new HashMap<>();/**entity -> pattern*/
            HashSet<Integer> classSet = new HashSet<>();//all classes
            ArrayList<ArrayList<Integer>> tripleList = new ArrayList<>();
            while (resultSet.next()){
                int sid = resultSet.getInt("subject");
                int pid = resultSet.getInt("predicate");
                int oid = resultSet.getInt("object");
                ArrayList<Integer> triple = new ArrayList<>(Arrays.asList(sid, pid, oid));
                tripleList.add(triple);
                HashSet<Integer> temp = id2EDP.getOrDefault(sid, new HashSet<>());
                if (typeID != 0 && pid == typeID){//S-TYPE-C
                    temp.add(oid);
                    classSet.add(oid);
                }
                else {
                    temp.add(pid);
                }
                id2EDP.put(sid, temp);
                if ((typeID == 0 || pid != typeID) && !literalSet.contains(oid)){//object is an entity
                    temp = id2EDP.getOrDefault(oid, new HashSet<>());
                    temp.add(-pid);
                    id2EDP.put(oid, temp);
                }
            }/**建完了entity -> pattern的Map*/
            int total = id2EDP.keySet().size();/**pattern的总数，也是entity的总数*/
            if (total == 0) {
                return false;
            }
            HashMap<HashSet<Integer>, Integer>pattern2Count = new HashMap<>(); /**pattern -> count*/
            for (int iter: id2EDP.keySet()){
                HashSet<Integer> pattern = id2EDP.get(iter);
                if (pattern2Count.containsKey(pattern)){
                    int temp = pattern2Count.get(pattern);
                    pattern2Count.put(pattern, temp + 1);
                }
                else {
                    pattern2Count.put(pattern, 1);
                }
            }
            /**patternMap finished*/
            List<Map.Entry<HashSet<Integer>, Integer>> toBeSortList = new ArrayList<>(pattern2Count.entrySet());
            Collections.sort(toBeSortList, new Comparator<Map.Entry<HashSet<Integer>, Integer>>() {
                @Override
                public int compare(Map.Entry<HashSet<Integer>, Integer> o1, Map.Entry<HashSet<Integer>, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
//            ArrayList<HashSet<Integer>> patternList = new ArrayList<>();
//            ArrayList<Integer> patternCount = new ArrayList<>();
//            for (HashSet<Integer> iter: pattern2Count.keySet()) {
//                patternList.add(iter);
//                patternCount.add(pattern2Count.get(iter));
//            }
//            patternList = sortAbyB(patternList, patternCount);
//            System.out.println(patternCount);
            HashMap<HashSet<Integer>, Integer> EDP2ID = new HashMap<>();
            int i = 0;
            for (Map.Entry<HashSet<Integer>, Integer> iter: toBeSortList) {
                i++;
                EDP2ID.put(iter.getKey(), i);
//                System.out.println(iter.getValue());////////////////////////////////////////////////////////////////////////////
            }
            String edpIndexFolder = "D:/Work/ISWC2021Index/EDPIndex/" + dataset + "/";
            File file = new File(edpIndexFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Directory directory = FSDirectory.open(Paths.get(edpIndexFolder));
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
            HashMap<Integer, Integer> edp2size = new HashMap<>();
            for (HashSet<Integer> pattern: EDP2ID.keySet()) {
                Document document = new Document();
                int edpId = EDP2ID.get(pattern);
                document.add(new IntPoint("id", edpId));
                document.add(new StoredField("id", edpId));
                int count = pattern2Count.get(pattern);
//                if (count != patternCount.get((edpId-1))) {
//                    System.out.println("WRONG!");
//                }
                document.add(new IntPoint("count", count));
                document.add(new StoredField("count", count));
                int edpSize = pattern.size();
                document.add(new IntPoint("size", edpSize));
                document.add(new StoredField("size", edpSize));
                edp2size.put(edpId, edpSize);
                StringBuilder inProp = new StringBuilder();
                StringBuilder outProp = new StringBuilder();
                StringBuilder classes = new StringBuilder();
                for (int id: pattern) {
                    if (classSet.contains(id)) {
                        classes.append(id).append(" ");
                    }
                    else if (id > 0) {
                        outProp.append(id).append(" ");
                    }
                    else {
                        inProp.append(-id).append(" ");
                    }
                }
                document.add(new TextField("classes", classes.toString().trim(), Field.Store.YES));
                document.add(new TextField("inProperty", inProp.toString().trim(), Field.Store.YES));
                document.add(new TextField("outProperty", outProp.toString().trim(), Field.Store.YES));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
            /** EDP index saved. */
            String entityIndexFolder = "D:/Work/ISWC2021Index/Entity2EDP/" + dataset + "/";
            file = new File(entityIndexFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            directory = FSDirectory.open(Paths.get(entityIndexFolder));
            indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            for (int entity: id2EDP.keySet()) {
                Document document = new Document();
                int edpId = EDP2ID.get(id2EDP.get(entity));
                document.add(new TextField("entity", String.valueOf(entity), Field.Store.YES));
                document.add(new TextField("edpId", String.valueOf(edpId), Field.Store.YES));
                int count = pattern2Count.get(id2EDP.get(entity));
                document.add(new IntPoint("count", count));
                document.add(new StoredField("count", count));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
            /** Entity -> EDP_id index saved. */

            HashMap<ArrayList<Integer>, ArrayList<Integer>> triple2LP = new HashMap<>();
            HashMap<ArrayList<Integer>, Integer> lp2Count = new HashMap<>();
            for (ArrayList<Integer> triple: tripleList) { // LP process
                int sid = triple.get(0);
                int pid = triple.get(1);
                int oid = triple.get(2);
                if (pid == typeID || literalSet.contains(oid)) {
                    continue;
                }
                ArrayList<Integer> lp = new ArrayList<>(Arrays.asList(EDP2ID.get(id2EDP.get(sid)), pid, EDP2ID.get(id2EDP.get(oid))));
                triple2LP.put(triple, lp);
                int count = lp2Count.getOrDefault(lp, 0);
                lp2Count.put(lp, (count + 1));
            }
            List<Map.Entry<ArrayList<Integer>, Integer>> toBeSortList2 = new ArrayList<>(lp2Count.entrySet());
            Collections.sort(toBeSortList2, new Comparator<Map.Entry<ArrayList<Integer>, Integer>>() {
                @Override
                public int compare(Map.Entry<ArrayList<Integer>, Integer> o1, Map.Entry<ArrayList<Integer>, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });

//            ArrayList<ArrayList<Integer>> lpList = new ArrayList<>();
//            ArrayList<Integer> lpCount = new ArrayList<>();
//            for (ArrayList<Integer> iter: lp2Count.keySet()) {
//                lpList.add(iter);
//                lpCount.add(lp2Count.get(iter));
//            }
//            lpList = sortAbyB2(lpList, lpCount);
            HashMap<ArrayList<Integer>, Integer> lp2ID = new HashMap<>();
            i = 0;
            for (Map.Entry<ArrayList<Integer>, Integer> iter: toBeSortList2) {
                i++;
                lp2ID.put(iter.getKey(), i);
//                System.out.println(iter.getValue());////////////////////////////////////////////////////////////////////////////
            }
            String lpIndexFolder = "D:/Work/ISWC2021Index/LPIndex/" + dataset + "/";
            file = new File(lpIndexFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            directory = FSDirectory.open(Paths.get(lpIndexFolder));
            indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            for (ArrayList<Integer> iter: lp2ID.keySet()) {
                Document document = new Document();
                int lpId = lp2ID.get(iter);
                document.add(new IntPoint("id", lpId));
                document.add(new StoredField("id", lpId));
                int count = lp2Count.get(iter);
//                if (count != lpCount.get(lpId - 1)) {
//                    System.out.println("WRONG!");
//                }
                document.add(new IntPoint("count", count));
                document.add(new StoredField("count", count));
                int lpSize = edp2size.get(iter.get(0)) + edp2size.get(iter.get(2)) - 1;
                document.add(new IntPoint("size", lpSize));
                document.add(new StoredField("size", lpSize));
                String lp = iter.get(0) + " " + iter.get(1) + " " + iter.get(2);
                document.add(new TextField("LP", lp, Field.Store.YES));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
            /** LP index saved. */
            String tripleFolder = "D:/Work/ISWC2021Index/Triple2LP/" + dataset + "/";
            file = new File(tripleFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            directory = FSDirectory.open(Paths.get(tripleFolder));
            indexWriterConfig = new IndexWriterConfig(new StandardAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            indexWriter = new IndexWriter(directory, indexWriterConfig);
            for (ArrayList<Integer> iter: triple2LP.keySet()) {
                Document document = new Document();
                String triple = iter.get(0) + " " + iter.get(1) + " " + iter.get(2);
                document.add(new TextField("triple", triple, Field.Store.YES));
                ArrayList<Integer> value = triple2LP.get(iter);
                String lp = value.get(0) + " " + value.get(1) + " " + value.get(2);
                document.add(new TextField("lp", lp, Field.Store.YES));
                int count = lp2Count.get(value);
                document.add(new IntPoint("count", count));
                document.add(new StoredField("count", count));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
            /** triple -> LP index saved. */
        } catch (Exception e){
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    /** Used in preprocess, part-2
     * @DATE: 2020/12/11
     * */
    private static boolean generateComponentIndex(int dataset) {
        Connection connection = new DBUtil().conn;
        try {
            HashMap<Integer, Integer> entity2edp = new HashMap<>();
            String edpPath = PATHS.ProjectData + "Entity2EDP/" + dataset + "/";
//            String edpPath = PATHS.wesleyBase + "Entity2EDP/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                int entity = Integer.parseInt(doc.get("entity"));
                int edp = Integer.parseInt(doc.get("edpId"));
                entity2edp.put(entity, edp);
            }
            reader.close();
            HashMap<String, Integer> lp2id = new HashMap<>();
            String lpPath = PATHS.ProjectData + "LPIndex/" + dataset + "/";
//            String lpPath = PATHS.wesleyBase + "LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                int lpId = Integer.parseInt(doc.get("id"));
                lp2id.put(doc.get("LP"), lpId);
            }
            reader.close();
            HashMap<String, Integer> triple2LocalId = new HashMap<>();
            HashMap<Integer, String> localId2lp = new HashMap<>();
            String triplePath = PATHS.ProjectData + "Triple2LP/" + dataset + "/";
//            String triplePath = PATHS.wesleyBase + "Triple2LP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(triplePath)));
            for (int i = 0; i < reader.maxDoc(); i++ ){
                Document doc = reader.document(i);
                triple2LocalId.put(doc.get("triple"), -(i+1));
                localId2lp.put(-(i+1), doc.get("lp"));
            }
            reader.close();
            //////////////////////////////////////////////////////////////////////////////////////////////
            String getType = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
            String getLabel = "select id, label, is_literal from uri_label_id? where dataset_local_id = ?";
            String select = "select subject, predicate, object from triple? where dataset_local_id = ?";
            PreparedStatement getTypeStatement = connection.prepareStatement(getType);
            PreparedStatement getLabelStatement = connection.prepareStatement(getLabel);
            PreparedStatement getTriple = connection.prepareStatement(select);
            ResultSet resultSet = getTypeStatement.executeQuery();
            resultSet.next();
            int typeID = resultSet.getInt("type_id");
            if (dataset <= 311) {
                getLabelStatement.setInt(1, 2);
                getLabelStatement.setInt(2, dataset);
                getTriple.setInt(1, 2);
                getTriple.setInt(2,  dataset);
            }
            else {
                getLabelStatement.setInt(1, 3);
                getLabelStatement.setInt(2, (dataset - 311));
                getTriple.setInt(1, 3);
                getTriple.setInt(2,  (dataset - 311));
            }
            HashMap<Integer, String> id2Label = new HashMap<>();
            HashSet<Integer> literalSet = new HashSet<>();// for literals
            resultSet = getLabelStatement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String label = resultSet.getString("label");
                if (label == null) {
                    id2Label.put(id, "");
                }
                else {
                    label = StringUtil.processLabel(resultSet.getString("label")); // the processed label!!!!
                    id2Label.put(id, label);
                }
                if (resultSet.getInt("is_literal") == 1) {
                    literalSet.add(id);
                }
            }
            HashMap<Integer, HashSet<ArrayList<Integer>>> entity2Triple = new HashMap<>(); // including subject and object
            Multigraph<Integer, DefaultEdge> ERgraph = new Multigraph<>(DefaultEdge.class);
            resultSet = getTriple.executeQuery();
            while (resultSet.next()) {
                int sid = resultSet.getInt("subject");
                int pid = resultSet.getInt("predicate");
                int oid = resultSet.getInt("object");
                if (pid == typeID || literalSet.contains(oid)) { // do not need to be added into the graph
                    ERgraph.addVertex(sid);
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                }
                else {
                    int link = triple2LocalId.get(sid + " " + pid + " " + oid);
                    ERgraph.addVertex(sid);
                    ERgraph.addVertex(link);
                    ERgraph.addVertex(oid);
                    ERgraph.addEdge(sid, link);
                    ERgraph.addEdge(link, oid);
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                    tempTripleSet = entity2Triple.getOrDefault(oid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(oid, tempTripleSet);
                }
            }
            ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(ERgraph);
            List<Set<Integer>> components = inspector.connectedSets();
            HashMap<Set<Integer>, TreeSet<Integer>> component2edp = new HashMap<>();
            HashMap<Set<Integer>, HashSet<ArrayList<Integer>>> component2triple = new HashMap<>();
            HashMap<Set<Integer>, TreeSet<Integer>> component2lp = new HashMap<>();
            HashMap<Set<Integer>, Integer> component2Size = new HashMap<>();
            for (Set<Integer> comp: components) {
                TreeSet<Integer> edpSet = new TreeSet<>(); //instantiated edps in the component
                HashSet<ArrayList<Integer>> tripleSet = new HashSet<>(); // involved triples in the component
                TreeSet<Integer> lpSet = new TreeSet<>(); // involved lps in the component
                for (int node: comp) {
                    if (entity2edp.containsKey(node)) {
                        edpSet.add(entity2edp.get(node));
                        tripleSet.addAll(entity2Triple.get(node));
                    }
                    if (localId2lp.containsKey(node)) {
                        lpSet.add(lp2id.get(localId2lp.get(node)));
                    }
                }
                component2edp.put(comp, edpSet);
                component2triple.put(comp, tripleSet);
                component2lp.put(comp, lpSet);
                component2Size.put(comp, edpSet.size() + lpSet.size());
            }
            List<Map.Entry<Set<Integer>, Integer>> toBeSortList = new ArrayList<>(component2Size.entrySet());
            Collections.sort(toBeSortList, new Comparator<Map.Entry<Set<Integer>, Integer>>() {
                @Override
                public int compare(Map.Entry<Set<Integer>, Integer> o1, Map.Entry<Set<Integer>, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
//            String componentFolder = PATHS.wesleyBase + "ComponentIndex/" + dataset + "/";
            File file = new File(componentFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Directory directory = FSDirectory.open(Paths.get(componentFolder));
            HashMap<String, Analyzer> perFieldAnalyzer = new HashMap<>();
            perFieldAnalyzer.put("text", new StemAnalyzer()); //Add analyzers for each field
            PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), perFieldAnalyzer);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
            int count = 0; // the i-th component
            for (Map.Entry<Set<Integer>, Integer> iter: toBeSortList) {
                Set<Integer> comp = iter.getKey();
                HashSet<ArrayList<Integer>> tripleSet = component2triple.get(comp);
                StringBuilder tripleStr = new StringBuilder();
                StringBuilder textStr = new StringBuilder();
                for (ArrayList<Integer> triple: tripleSet) {
                    tripleStr.append(triple.get(0)).append(" ").append(triple.get(1)).append(" ").append(triple.get(2)).append(",");
                    textStr.append(id2Label.get(triple.get(0))).append(" ").append(id2Label.get(triple.get(1))).append(" ").append(id2Label.get(triple.get(2))).append(" ");
                }
                TreeSet<Integer> edpList = component2edp.get(comp);
                StringBuilder edpStr = new StringBuilder();
                for (int edp: edpList) {
                    edpStr.append(edp).append(" ");
                }
                TreeSet<Integer> lpList = component2lp.get(comp);
                StringBuilder lpStr = new StringBuilder();
                for (int lp: lpList) {
                    lpStr.append(lp).append(" ");
                }
                Document document = new Document();
                count++;
                document.add(new IntPoint("id", count));
                document.add(new StoredField("id", count));
                document.add(new TextField("triple", tripleStr.substring(0, tripleStr.length() - 1), Field.Store.YES));
                document.add(new TextField("text", textStr.toString().trim(), Field.Store.NO)); // Field.Store.YES except for 310
                document.add(new TextField("edp", edpStr.toString().trim(), Field.Store.YES));
                document.add(new TextField("lp", lpStr.toString().trim(), Field.Store.YES));
                indexWriter.addDocument(document);
            }
            indexWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    /** Generate label-id index for all datasets.
     * @DATE: 2020/11/29
     * */
    private static boolean generateLabelIndex(int dataset) {
        Connection connection = new DBUtil().conn;
        String select = "select label, id from uri_label_id? where dataset_local_id = ? order by id";
        ArrayList<Integer> idList = new ArrayList<>();
        ArrayList<String> labelList = new ArrayList<>();
        try {
            PreparedStatement selectStatement = connection.prepareStatement(select);
            if (dataset <= 311) {
                selectStatement.setInt(1, 2);
                selectStatement.setInt(2, dataset);
            }
            else {
                selectStatement.setInt(1, 3);
                selectStatement.setInt(2, (dataset - 311));
            }
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                idList.add(id);
                String label = resultSet.getString("label");
                if (label == null) {
                    label = "";
                }
                else {
                    label = StringUtil.processLabel(label);
                }
                labelList.add(label);
            }
            String indexFolder = "D:/Work/ISWC2021Index/LabelIdIndex/" + dataset + "/";
            File file = new File(indexFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Directory directory = FSDirectory.open(Paths.get(indexFolder));
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(new StemAnalyzer());
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter writer = new IndexWriter(directory, indexWriterConfig);
            for (int i = 0; i < idList.size(); i++) {
                Document document = new Document();
                int id = idList.get(i);
                document.add(new IntPoint("id", id));
                document.add(new StringField("id", String.valueOf(id), Field.Store.YES));
                document.add(new TextField("label", labelList.get(i), Field.Store.YES));
                writer.addDocument(document);
            }
            writer.close();
            directory.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return true;
    }

    /** Generating indexes for EDP, LP, components, cost several hours for all 9,544 datasets.
     * @DATE: 2020/11
     * */
    public static void preprocess(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger("src/xxwang/file/dataset.txt", "\t");
//        ArrayList<ArrayList<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
//            boolean succ = generatePatternIndex(dataset);
            boolean succ = generateComponentIndex(dataset);
//            boolean succ = generateLabelIndex(dataset);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

    /** Elements to be covered: EDP, LP
     * @param dataset : dataset id
     * @DATE: 2020/12/11
     * */
    public static boolean getSetCoverComponents(int dataset) {
        try {
            String edpPath = "D:/Work/ISWC2021Index/EDPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            int edpSize = reader.maxDoc(); // amount of EDPs, id = {1, ..., edpSize}
            reader.close();
            String lpPath = "D:/Work/ISWC2021Index/LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            int lpSize = reader.maxDoc(); // amount of LPs, id = {1, ..., lpSize}
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2edp = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> component2lp = new HashMap<>();
            String componentFolder = "D:/Work/ISWC2021Index/ComponentIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder)));
            for (int i = 0; i < reader.maxDoc(); i++){
                Document doc = reader.document(i);
                int id = Integer.parseInt(doc.get("id"));
                HashSet<Integer> edpSet = splitAsInteger(doc.get("edp"), ' ');
                HashSet<Integer> lpSet = splitAsInteger(doc.get("lp"), ' ');
                component2edp.put(id, edpSet);
                component2lp.put(id, lpSet);
            }
            reader.close();
            /**finish preparing sets. */
//            System.out.println(edpSize);////////////////////////////////////////////////////////////////////////////
//            System.out.println(lpSize);
//            System.out.println(component2edp);
//            System.out.println(component2lp);
            ArrayList<Integer> SCPResult = new ArrayList<>();
            SCPResult.add(1);
            HashSet<Integer> coveredEDP = component2edp.get(1);
            HashSet<Integer> coveredLP = component2lp.get(1);
            HashSet<Integer> currentEDP = component2edp.get(1);
            HashSet<Integer> currentLP = component2lp.get(1);
            component2edp.remove(1);
            component2lp.remove(1);
//            System.out.println(coveredEDP);////////////////////////////////////////////////////////////////////////////
//            System.out.println(coveredLP);
//            System.out.println(currentEDP);
//            System.out.println(currentLP);
            while (coveredEDP.size() < edpSize || coveredLP.size() < lpSize) {
//                System.out.print(coveredEDP.size() + " ==== " + edpSize + "  ");
//                System.out.println(coveredLP.size() + " ==== " + lpSize);
                int maxSize = 0;
                int maxComp = 0; // to record the component with the most |edp|+|lp|
                for (Map.Entry<Integer, HashSet<Integer>> iter: component2edp.entrySet()) {
                    int comp = iter.getKey();
                    iter.getValue().removeAll(currentEDP);
                    component2lp.get(comp).removeAll(currentLP);
                    int size = iter.getValue().size() + component2lp.get(comp).size();
                    if (maxSize < size) {
                        maxComp = comp;
                        maxSize = size;
                    }
                }
                SCPResult.add(maxComp);
                currentEDP = component2edp.get(maxComp);
                coveredEDP.addAll(currentEDP);
                currentLP = component2lp.get(maxComp);
                coveredLP.addAll(currentLP);
                component2edp.remove(maxComp);
                component2lp.remove(maxComp);
//                System.out.println(coveredEDP);////////////////////////////////////////////////////////////////////////////
//                System.out.println(coveredLP);
//                System.out.println(currentEDP);
//                System.out.println(currentLP);
            }
            StringBuilder compStr = new StringBuilder();
            for (int iter: SCPResult) {
                compStr.append(iter).append(" ");
            }
            String resultFolder = "D:/Work/ISWC2021Index/SCPResult/" + dataset + ".txt";
            File file = new File(resultFolder);
            if (!file.exists()) {
                file.createNewFile();
            }
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(compStr.toString().trim());
            writer.write("\n");
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**Get SCP result for all datasets.
     * @DATE: 2020/11
     */
    public static void process(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger(PATHS.ProjectData + "file/datasetSC.txt", "\t");
//        ArrayList<ArrayList<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
            boolean succ = getSetCoverComponents(dataset);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

    /** Used in SCP.
     * @DATE: 2020/11
     * */
    private static HashSet<Integer> splitAsInteger(String str, char regex) {
        HashSet<Integer> result = new HashSet<>();
        String splitStr = "";
        int length = str.length();
        int i = 0, begin = 0;
        for (i = 0; i < length; i++) {
            if (str.charAt(i) == regex) {
                splitStr = str.substring(begin, i);
                result.add(Integer.parseInt(splitStr));
                str = str.substring(i+1, length);
                length = str.length();
                i = 0;
            }
        }
        if (!str.isEmpty()) {
            result.add(Integer.parseInt(str));
        }
        return result;
    }

    /** Elements to be covered: EDP, LP, Keywords
     * @DATE: 2020/11
     * @param dataset : dataset id
     */
    private static boolean getSetCoverComponents(int pair, int dataset, List<String> keywords) {
        try {
            String edpPath = PATHS.ProjectData + "EDPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            int edpSize = reader.maxDoc(); // amount of EDPs, id = {1, ..., edpSize}
            reader.close();
            String lpPath = PATHS.ProjectData + "LPIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            int lpSize = reader.maxDoc(); // amount of LPs, id = {1, ..., lpSize}
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2edp = new HashMap<>();
            HashMap<Integer, HashSet<Integer>> component2lp = new HashMap<>();
            String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder)));
            for (int i = 0; i < reader.maxDoc(); i++){
                Document doc = reader.document(i);
                int id = Integer.parseInt(doc.get("id"));
                HashSet<Integer> edpSet = splitAsInteger(doc.get("edp"), ' ');
                HashSet<Integer> lpSet = splitAsInteger(doc.get("lp"), ' ');
                component2edp.put(id, edpSet);
                component2lp.put(id, lpSet);
            }
            reader.close();
            HashMap<Integer, HashSet<Integer>> component2Key = new HashMap<>();
            int keywordSize = 0; //search for keywords
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
            for (int i = 0; i < keywords.size(); i++) {
                String keyword = keywords.get(i);
                QueryParser parser = new QueryParser("text", new StemAnalyzer());
                Query query = parser.parse(keyword);
                TopDocs docs = searcher.search(query, 100000000);
                ScoreDoc[] scores = docs.scoreDocs;
                if (scores.length == 0) {
                    System.out.println("KEYWORD: " + keyword + " NO HIT!!");
                    continue;
                }
                keywordSize++;
                for (ScoreDoc score : scores) {
                    int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                    HashSet<Integer> tempValue = component2Key.getOrDefault(id, new HashSet<>());
                    tempValue.add(i);
                    component2Key.put(id, tempValue);
                }
            }
            /**finish preparing sets. */
            ArrayList<Integer> SCPResult = new ArrayList<>();
            HashSet<Integer> coveredEDP = new HashSet<>();
            HashSet<Integer> coveredLP = new HashSet<>();
            HashSet<Integer> coveredKey = new HashSet<>();
            HashSet<Integer> currentEDP = new HashSet<>();
            HashSet<Integer> currentLP = new HashSet<>();
            HashSet<Integer> currentKey = new HashSet<>();
            while (coveredEDP.size() < edpSize || coveredLP.size() < lpSize || coveredKey.size() < keywordSize) {
//                System.out.print(coveredEDP.size() + " ==== " + edpSize + "  ");
//                System.out.println(coveredLP.size() + " ==== " + lpSize);
                int maxSize = 0;
                int maxComp = 0; // to record the component with the most |edp|+|lp|+|Key|
                for (Map.Entry<Integer, HashSet<Integer>> iter: component2edp.entrySet()) {
                    int comp = iter.getKey();
                    iter.getValue().removeAll(currentEDP);
                    component2lp.get(comp).removeAll(currentLP);
                    int size = iter.getValue().size() + component2lp.get(comp).size();
                    if (component2Key.containsKey(comp)) {
                        component2Key.get(comp).removeAll(currentKey);
                        size += component2Key.get(comp).size();
                    }
                    if (maxSize < size) {
                        maxComp = comp;
                        maxSize = size;
                    }
                }
                SCPResult.add(maxComp);
                currentEDP = component2edp.get(maxComp);
                coveredEDP.addAll(currentEDP);
                currentLP = component2lp.get(maxComp);
                coveredLP.addAll(currentLP);
                currentKey = component2Key.getOrDefault(maxComp, new HashSet<>());
                coveredKey.addAll(currentKey);
                component2edp.remove(maxComp);
                component2lp.remove(maxComp);
                component2Key.remove(maxComp);
            }
            StringBuilder compStr = new StringBuilder();
            for (int iter: SCPResult) {
                compStr.append(iter).append(" ");
            }
            String resultFolder = PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt";
            File file = new File(resultFolder);
            if (file.exists()) {
                System.out.println("File EXIST!!");
                return false;
            }
            file.createNewFile();
            BufferedWriter writer = new BufferedWriter(new FileWriter(file));
            writer.write(compStr.toString().trim());
            writer.write("\n");
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**Get SCP result [INCLUDING KEYWORDS!!] for all datasets.
     * @DATE: 2020/11
     */
    public static void processWithKeyword(int start, int end) {
        ArrayList<String> queryPair = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(new File(PATHS.ProjectData + "file/QueryPair.txt")))){
            String line = null;
            while ((line = reader.readLine()) != null) {
                queryPair.add(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (int i = 0; i < queryPair.size(); i++) {
            String iter = queryPair.get(i);
            int dataset = Integer.parseInt(iter.split("\t")[0]);
            if (dataset < start || dataset > end || dataset == 311) {
                continue;
            }
            ArrayList<String> keyword = new ArrayList<>(Arrays.asList(iter.split("\t")[4].split(" ")));
            boolean succ = getSetCoverComponents(i, dataset, keyword);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

//    public static void main(String[] args) {
//        generateComponentIndex(21);
//        getSetCoverComponents(8);
//        preprocess(0, 309); // 310, 311: on wesley 2020-12-11
//        process(0, 311); // 2020-12-12
//        processWithKeyword(0, 9630);
//    }

}
