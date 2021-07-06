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
import PCSG.PATHS;
import PCSG.util.*;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class KeyKGIndexer {

    public static boolean buildIndex(int dataset, int component) {
        /**graph: S-P-O URI
         * subName: node URI --> node Id
         * invertTable: keyword String --> node id containing it
         * KeyMap: keyword id --> keyword String
         * File Location: D:/Work/ISWC2021Index/KeyKGPNoKeyword/
         * @DATE:2020/11/28
         */
        try {
            HashMap<Integer, String> id2lp = new HashMap<>();
            String lpPath = "D:/Work/ISWC2021Index/LPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document doc = reader.document(i);
                int lpId = Integer.parseInt(doc.get("id"));
                String lpStr = doc.get("LP");
                id2lp.put(lpId, lpStr);
            }
            reader.close();
            HashMap<String, String> entity2edp = new HashMap<>();
            String edpPath = "D:/Work/ISWC2021Index/Entity2EDP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                entity2edp.put(document.get("entity"), document.get("edpId"));
            }
            reader.close();
            HashMap<String, String> triple2lp = new HashMap<>();
            String triplePath = "D:/Work/ISWC2021Index/Triple2LP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(triplePath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                triple2lp.put(document.get("triple"), document.get("lp"));
            }
            reader.close();
            String[] edpStr = IndexUtil.getFieldValue("D:/Work/ISWC2021Index/ComponentIndex/" + dataset + "/", "id", component, "edp").split(" ");
            String lpIdStr = IndexUtil.getFieldValue("D:/Work/ISWC2021Index/ComponentIndex/" + dataset + "/", "id", component, "lp"); // might be empty
            ArrayList<String> keyList = new ArrayList<>(Arrays.asList(edpStr));
            if (lpIdStr.length() > 0) {
                for (String id: lpIdStr.split(" ")) {
                    keyList.add(id2lp.get(Integer.parseInt(id)));
                }
            }
            String[] triples = IndexUtil.getFieldValue("D:/Work/ISWC2021Index/ComponentIndex/" + dataset + "/", "id", component, "triple").split(",");
            String baseFolder = "D:/Work/ISWC2021Index/KeyKGPNoKeyword/";
            HashSet<String> nodeSet = new HashSet<>();
            HashMap<String, HashSet<String>> invertedMap = new HashMap<>();
            //========graph========
            File file = new File(baseFolder + dataset + "-" + component + "/");
            if (!file.exists()) {
                file.mkdirs();
            }
            String graphFile = baseFolder + dataset + "-" + component + "/graph.txt";
            file = new File(graphFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter graphWriter = new PrintWriter(graphFile);
            for (String iter: triples) {
                graphWriter.println(iter);
                String[] spo = iter.split(" ");
                if (triple2lp.containsKey(iter)) {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                    nodeSet.add(spo[2]);
                    String edp2 = entity2edp.get(spo[2]);
                    tempValue = invertedMap.getOrDefault(edp2, new HashSet<>());
                    tempValue.add(spo[2]);
                    invertedMap.put(edp2, tempValue);
                    nodeSet.add(iter);
                    String lp = triple2lp.get(iter);
                    tempValue = invertedMap.getOrDefault(lp, new HashSet<>());
                    tempValue.add(iter);
                    invertedMap.put(lp, tempValue);
                }
                else {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                }
            }
            graphWriter.close();
            //========subName========
            HashMap<String, Integer> node2Id = new HashMap<>();
            String subNameFile = baseFolder + dataset + "-" + component + "/subName.txt";
            file = new File(subNameFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter subNameWriter = new PrintWriter(subNameFile);
            int count = 0;
            for (String iter: nodeSet) {
                subNameWriter.println(iter);
                node2Id.put(iter, count);
                count++;
            }
            subNameWriter.close();
            //========keyMap========
            String keyMapFile = baseFolder + dataset + "-" + component + "/keyMap.txt";
            file = new File(keyMapFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter keyMapWriter = new PrintWriter(keyMapFile);
            for (String iter: keyList) {
                keyMapWriter.println(iter);
            }
            keyMapWriter.close();
            //========InvertedTable========
            String invertedTableFile = baseFolder + dataset + "-" + component + "/invertedTable.txt";
            file = new File(invertedTableFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter invTableWriter = new PrintWriter(invertedTableFile);
            for (Map.Entry<String, HashSet<String>> iter: invertedMap.entrySet()) {
                HashSet<String> values = iter.getValue();
                StringBuilder content = new StringBuilder();
                for (String v: values) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println(iter.getKey() + ":" + content.toString().trim());
            }
            invTableWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void processNoKeyword(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger("src/xxwang/file/dataset.txt", "\t");
//        ArrayList<ArrayList<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
            File file = new File("D:/Work/ISWC2021Index/SCPResult/" + dataset + ".txt");
            if (!file.exists()) {
                continue;
            }
            List<List<Integer>> comps  = ReadFile.readInteger("D:/Work/ISWC2021Index/SCPResult/" + dataset + ".txt", " ");
            for (int iter: comps.get(0)) {
                boolean succ = buildIndex(dataset, iter);
                System.out.println(dataset + "-th dataset " + iter + "-th component: " + succ);
            }
        }
    }

    private static boolean generateNodeLabelIndex (int dataset) {
        String getType = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
        String getLabel = "select id, label, is_literal from uri_label_id? where dataset_local_id = ?";
        String select = "select subject, predicate, object from triple? where dataset_local_id = ?";
        Connection connection = new DBUtil().conn;
        try {
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
            resultSet = getTriple.executeQuery();
            while (resultSet.next()) {
                int sid = resultSet.getInt("subject");
                int pid = resultSet.getInt("predicate");
                int oid = resultSet.getInt("object");
                if (pid == typeID || literalSet.contains(oid)) { // do not need to be added into the graph
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                }
                else {
                    HashSet<ArrayList<Integer>> tempTripleSet = entity2Triple.getOrDefault(sid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(sid, tempTripleSet);
                    tempTripleSet = entity2Triple.getOrDefault(oid, new HashSet<>());
                    tempTripleSet.add(new ArrayList<>(Arrays.asList(sid, pid, oid)));
                    entity2Triple.put(oid, tempTripleSet);
                }
            }
            ArrayList<Integer> entityList = new ArrayList<>();
            String edpPath = PATHS.ProjectData + "Entity2EDP/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                entityList.add(Integer.parseInt(document.get("entity")));
            }
            reader.close();
            String nodeLabelFolder = PATHS.ProjectData + "NodeLabelIndex/" + dataset + "/";
            File file = new File(nodeLabelFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Directory directory = FSDirectory.open(Paths.get(nodeLabelFolder));
            HashMap<String, Analyzer> perFieldAnalyzer = new HashMap<>();
            perFieldAnalyzer.put("text", new StemAnalyzer()); //Add analyzers for each field
            PerFieldAnalyzerWrapper analyzer = new PerFieldAnalyzerWrapper(new StandardAnalyzer(), perFieldAnalyzer);
            IndexWriterConfig indexWriterConfig = new IndexWriterConfig(analyzer);
            indexWriterConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
            IndexWriter indexWriter = new IndexWriter(directory, indexWriterConfig);
            for (int entity: entityList) {
                Document document = new Document();
                document.add(new IntPoint("node", entity));
                document.add(new StoredField("node", String.valueOf(entity)));
                HashSet<String> wordSet = new HashSet<>();
                StringBuilder tripleStr = new StringBuilder();
                for (ArrayList<Integer> iter : entity2Triple.get(entity)) {
                    wordSet.addAll(Arrays.asList(id2Label.get(iter.get(0)).split("\\s+")));
                    wordSet.addAll(Arrays.asList(id2Label.get(iter.get(1)).split("\\s+")));
                    wordSet.addAll(Arrays.asList(id2Label.get(iter.get(2)).split("\\s+")));
                    tripleStr.append(iter.get(0)).append(" ").append(iter.get(1)).append(" ").append(iter.get(2)).append(",");
                }
                StringBuilder textStr = new StringBuilder();
                for (String iter: wordSet) {
                    textStr.append(iter).append(" ");
                }
                document.add(new TextField("text", textStr.toString().trim(), Field.Store.YES));
                document.add(new TextField("triple", tripleStr.substring(0, tripleStr.length() - 1), Field.Store.YES));
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

    public static void preprocess(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger(PATHS.ProjectData + "file/dataset.txt", "\t");
        for (int dataset: datasets.get(0)) {
            if (dataset < start || dataset > end) {
                continue;
            }
            boolean succ = generateNodeLabelIndex(dataset);
            System.out.println(dataset + "-th dataset: " + succ);
        }
    }

    public static boolean buildIndexWithKeywords(int pair, int dataset, int component, List<String> keys, List<String> keywords) {
        /**graph: S-P-O URI (same)
         * subName: node URI --> node Id (same)
         * invertTable: keyword String --> node id containing it (different)
         * KeyMap: keyword id --> keyword String (different)
         * File Location: D:/Work/ISWC2021Index/KeyKGPWithKeyword/
         * @DATE:2020/12/2
         */
        try {
            HashMap<String, String> entity2edp = new HashMap<>();
            String edpPath = PATHS.ProjectData + "Entity2EDP/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                entity2edp.put(document.get("entity"), document.get("edpId"));
            }
            reader.close();
            HashMap<String, String> triple2lp = new HashMap<>();
            String triplePath = PATHS.ProjectData + "Triple2LP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(triplePath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                triple2lp.put(document.get("triple"), document.get("lp"));
            }
            reader.close();
            //========Prepare the keyword Map========
            HashMap<String, HashSet<Integer>> keyword2Id = new HashMap<>(); // all nodes
            HashMap<String, HashSet<String>> keyword2Node = new HashMap<>();
            String labelIdFolder = PATHS.ProjectData + "NodeLabelIndex/" + dataset + "/";
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(labelIdFolder))));
            for (String iter: keywords){ // if empty, nothing to add.
                QueryParser parser = new QueryParser("text", new StemAnalyzer());
                Query query = parser.parse(iter);
                TopDocs docs = searcher.search(query, 100000000);
                ScoreDoc[] scores = docs.scoreDocs;
                HashSet<Integer> hitId = new HashSet<>();
                for (ScoreDoc score: scores) {
                    int id = Integer.parseInt(searcher.doc(score.doc).get("node"));
                    hitId.add(id);
                }
                keyword2Id.put(iter, hitId);
                keyword2Node.put(iter, new HashSet<>());
            }
            //========finish. ========
            String[] triples = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", component, "triple").split(",");
            String baseFolder = PATHS.ProjectData + "KeyKGPWithKeyword/" + pair + "-" + dataset + "-" + component;
            HashSet<String> nodeSet = new HashSet<>();
            HashMap<String, HashSet<String>> invertedMap = new HashMap<>();
            //========graph========
            File file = new File(baseFolder + "/");
            if (!file.exists()) {
                file.mkdirs();
            }
            String graphFile = baseFolder + "/graph.txt";
            file = new File(graphFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter graphWriter = new PrintWriter(graphFile);
            for (String iter: triples) {
                graphWriter.println(iter);
                String[] spo = iter.split(" ");
                if (triple2lp.containsKey(iter)) {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[0]))) {
                            keyword2Node.get(kwd).add(spo[0]);
                            keyword2Node.get(kwd).add(iter);
                        }
                    } ////////////////////////////////////////////////
                    nodeSet.add(spo[2]);
                    String edp2 = entity2edp.get(spo[2]);
                    tempValue = invertedMap.getOrDefault(edp2, new HashSet<>());
                    tempValue.add(spo[2]);
                    invertedMap.put(edp2, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[2]))) {
                            keyword2Node.get(kwd).add(spo[2]);
                            keyword2Node.get(kwd).add(iter);
                        }
                    }//////////////////////////////////////////////
                    nodeSet.add(iter);
                    String lp = triple2lp.get(iter);
                    tempValue = invertedMap.getOrDefault(lp, new HashSet<>());
                    tempValue.add(iter);
                    invertedMap.put(lp, tempValue);
                }
                else {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[0]))) {
                            keyword2Node.get(kwd).add(spo[0]);
                        }
                    }
                }
            }
            graphWriter.close();
            //========subName========
            HashMap<String, Integer> node2Id = new HashMap<>();
            String subNameFile = baseFolder + "/subName.txt";
            file = new File(subNameFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter subNameWriter = new PrintWriter(subNameFile);
            int count = 0;
            for (String iter: nodeSet) {
                Document document = new Document();
                subNameWriter.println(iter);
                node2Id.put(iter, count);
                count++;
            }
            subNameWriter.close();
            //========keyMap========
            String keyMapFile = baseFolder + "/keyMap.txt";
            file = new File(keyMapFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter keyMapWriter = new PrintWriter(keyMapFile);
            for (String iter: keys) {
                keyMapWriter.println(iter);
            }
            for (String iter: keywords) {
                keyMapWriter.println("\"" + iter + "\"");
            }
            keyMapWriter.close();
            //========InvertedTable========
            String invertedTableFile = baseFolder + "/invertedTable.txt";
            file = new File(invertedTableFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter invTableWriter = new PrintWriter(invertedTableFile);
            for (Map.Entry<String, HashSet<String>> iter: invertedMap.entrySet()) {
                StringBuilder content = new StringBuilder();
                for (String v: iter.getValue()) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println(iter.getKey() + ":" + content.toString().trim());
            }
            for (Map.Entry<String, HashSet<String>> iter: keyword2Node.entrySet()) {
                StringBuilder content = new StringBuilder();
                for (String v: iter.getValue()) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println("\"" + iter.getKey() + "\"" + ":" + content.toString().trim());
            }
            invTableWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static boolean buildIndexWithAllWords(int dataset, int component, List<String> keywords) {
        /**graph: S-P-O URI (same)
         * subName: node URI --> node Id (same)
         * invertTable: keyword String --> node id containing it (different)
         * KeyMap: keyword id --> keyword String (different)
         * File Location: D:/Work/ISWC2021Index/KeyKGPWithKeyword/
         * @DATE:2020/12/2
         */
        try {
            HashMap<Integer, String> id2lp = new HashMap<>();
            String lpPath = PATHS.ProjectData + "LPIndex/" + dataset + "/";
            IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(lpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document doc = reader.document(i);
                int lpId = Integer.parseInt(doc.get("id"));
                String lpStr = doc.get("LP");
                id2lp.put(lpId, lpStr);
            }
            reader.close();
            HashMap<String, String> entity2edp = new HashMap<>();
            String edpPath = PATHS.ProjectData + "Entity2EDP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(edpPath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                entity2edp.put(document.get("entity"), document.get("edpId"));
            }
            reader.close();
            HashMap<String, String> triple2lp = new HashMap<>();
            String triplePath = PATHS.ProjectData + "Triple2LP/" + dataset + "/";
            reader = DirectoryReader.open(FSDirectory.open(Paths.get(triplePath)));
            for (int i = 0; i < reader.maxDoc(); i++) {
                Document document = reader.document(i);
                triple2lp.put(document.get("triple"), document.get("lp"));
            }
            reader.close();
            //========Prepare the keyword Map========
            HashMap<String, HashSet<Integer>> keyword2Id = new HashMap<>(); // all nodes
            HashMap<String, HashSet<String>> keyword2Node = new HashMap<>();
            String labelIdFolder = PATHS.ProjectData + "NodeLabelIndex/" + dataset + "/";
            IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(labelIdFolder))));
            for (String iter: keywords){ // if empty, nothing to add.
                QueryParser parser = new QueryParser("text", new StemAnalyzer());
                Query query = parser.parse(iter);
                TopDocs docs = searcher.search(query, 100000000);
                ScoreDoc[] scores = docs.scoreDocs;
                HashSet<Integer> hitId = new HashSet<>();
                for (ScoreDoc score: scores) {
                    int id = Integer.parseInt(searcher.doc(score.doc).get("node"));
                    hitId.add(id);
                }
                keyword2Id.put(iter, hitId);
                keyword2Node.put(iter, new HashSet<>());
            }
            //========finish. ========
            String[] triples = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", component, "triple").split(",");
            String baseFolder = PATHS.ProjectData + "KeyKGPWithKeyword/" + dataset + "-" + component;
            HashSet<String> nodeSet = new HashSet<>();
            HashMap<String, HashSet<String>> invertedMap = new HashMap<>();
            //========graph========
            File file = new File(baseFolder + "/");
            if (!file.exists()) {
                file.mkdirs();
            }
            String graphFile = baseFolder + "/graph.txt";
            file = new File(graphFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter graphWriter = new PrintWriter(graphFile);
            for (String iter: triples) {
                graphWriter.println(iter);
                String[] spo = iter.split(" ");
                if (triple2lp.containsKey(iter)) {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[0]))) {
                            keyword2Node.get(kwd).add(spo[0]);
                            keyword2Node.get(kwd).add(iter);
                        }
                    } ////////////////////////////////////////////////
                    nodeSet.add(spo[2]);
                    String edp2 = entity2edp.get(spo[2]);
                    tempValue = invertedMap.getOrDefault(edp2, new HashSet<>());
                    tempValue.add(spo[2]);
                    invertedMap.put(edp2, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[2]))) {
                            keyword2Node.get(kwd).add(spo[2]);
                            keyword2Node.get(kwd).add(iter);
                        }
                    }//////////////////////////////////////////////
                    nodeSet.add(iter);
                    String lp = triple2lp.get(iter);
                    tempValue = invertedMap.getOrDefault(lp, new HashSet<>());
                    tempValue.add(iter);
                    invertedMap.put(lp, tempValue);
                }
                else {
                    nodeSet.add(spo[0]);
                    String edp0 = entity2edp.get(spo[0]);
                    HashSet<String> tempValue = invertedMap.getOrDefault(edp0, new HashSet<>());
                    tempValue.add(spo[0]);
                    invertedMap.put(edp0, tempValue);
                    for (Map.Entry<String, HashSet<Integer>> entryIter: keyword2Id.entrySet()) {
                        String kwd = entryIter.getKey();
                        if (entryIter.getValue().contains(Integer.parseInt(spo[0]))) {
                            keyword2Node.get(kwd).add(spo[0]);
                        }
                    }
                }
            }
            graphWriter.close();
            //========subName========
            HashMap<String, Integer> node2Id = new HashMap<>();
            String subNameFile = baseFolder + "/subName.txt";
            file = new File(subNameFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter subNameWriter = new PrintWriter(subNameFile);
            int count = 0;
            for (String iter: nodeSet) {
                Document document = new Document();
                subNameWriter.println(iter);
                node2Id.put(iter, count);
                count++;
            }
            subNameWriter.close();
            //========keyMap and InvertedTable========
            String keyMapFile = baseFolder + "/keyMap.txt";
            file = new File(keyMapFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter keyMapWriter = new PrintWriter(keyMapFile);
            String[] edps = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", component, "edp").split(" ");
            for (String iter: edps) {
                keyMapWriter.println(iter);
            }
            String lpStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", component, "lp");
            if (lpStr.length() > 0) {
                for (String iter: lpStr.split(" ")) {
                    keyMapWriter.println(id2lp.get(Integer.parseInt(iter)));
                }
            }
            for (String iter: keywords) {
                keyMapWriter.println("\"" + iter + "\"");
            }
            keyMapWriter.close();
            //========InvertedTable========
            String invertedTableFile = baseFolder + "/invertedTable.txt";
            file = new File(invertedTableFile);
            if (!file.exists()) {
                file.createNewFile();
            }
            PrintWriter invTableWriter = new PrintWriter(invertedTableFile);
            for (Map.Entry<String, HashSet<String>> iter: invertedMap.entrySet()) {
                StringBuilder content = new StringBuilder();
                for (String v: iter.getValue()) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println(iter.getKey() + ":" + content.toString().trim());
            }
            for (Map.Entry<String, HashSet<String>> iter: keyword2Node.entrySet()) {
                StringBuilder content = new StringBuilder();
                for (String v: iter.getValue()) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println("\"" + iter.getKey() + "\"" + ":" + content.toString().trim());
            }
            invTableWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public static void processWithKeyword(int start, int end) {
        List<List<String>> pair = ReadFile.readString(PATHS.ProjectData + "file/KeysWithKeywords.txt", "\t");
        List<List<String>> keywords = ReadFile.readString(PATHS.ProjectData + "file/KeysOnlyKeywords.txt", "\t");
        for (int i = 0; i < pair.size(); i++) {
            List<String> iter = pair.get(i);
            String[] ids = iter.get(0).split("-");
            int pairId = Integer.parseInt(ids[0]);
            int dataset = Integer.parseInt(ids[1]);
            int component = Integer.parseInt(ids[2]);
            if (dataset < start || dataset > end) {
                continue;
            }
            List<String> keyword = keywords.get(i);
            keyword.remove(0);
            List<String> keys = iter.subList(1, iter.size() - keyword.size());
            boolean result = buildIndexWithKeywords(pairId, dataset, component, keys, keyword);
            System.out.println(iter.get(0) + ": " + result);
        }
    }

    public static void processWithAllWord(int start, int end) {
        List<List<String>> pair = ReadFile.readString(PATHS.ProjectData + "file/CasesWithKeywords.txt", "\t");
        for (List<String> iter: pair) {
            String[] ids = iter.get(0).split("-");
            int dataset = Integer.parseInt(ids[0]);
            int component = Integer.parseInt(ids[1]);
            if (dataset < start || dataset > end) {
                continue;
            }
            List<String> AllKeyword = iter.subList(1, iter.size());
            boolean result = buildIndexWithAllWords(dataset, component, AllKeyword);
            System.out.println(dataset + "-" + component + ": " + result);
        }
    }

//    public static void main(String[] args) {
//        processNoKeyword(0, 1000);
//        preprocess(0, 9630);
//        processWithKeyword(0, 106);
//        processWithAllWord(0, 9630);
//    }
}
