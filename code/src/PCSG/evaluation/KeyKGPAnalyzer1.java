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
import PCSG.util.DBUtil;
import PCSG.util.ReadFile;
import PCSG.util.StemAnalyzer;

import java.io.*;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class KeyKGPAnalyzer1 {

    private static Set<String> tree2Triple(String treeFile, String baseFile) {
        Set<String> result = new HashSet<>();
        List<String> edges = ReadFile.readString(treeFile);
        List<String> labels = ReadFile.readString(baseFile);
        if (edges.get(0).equals("")) {
            return result;
        }
        for (String edge: edges.get(0).split(",")) {
            int from = Integer.parseInt(edge.split(" ")[0]);
            int to = Integer.parseInt(edge.split(" ")[1]);
            String node1 = labels.get(from);
            String node2 = labels.get(to);
//            System.out.println(node1 + "  ---->  " + node2);
            if (node1.contains(" ")) {
                result.add(node1);
            }
            else result.add(node2);
        }
        return result;
    }

    // used when a ansTree contains only one node but no edges
    private static String node2TripleOrNode(String node, String baseFile) {
        List<String> labels = ReadFile.readString(baseFile);
        return labels.get(Integer.parseInt(node));
    }

    public static void getResultTriples(String onlyKeyFile, String resultFolder1, String resultFolder2, String baseFolder1, String baseFolder2, String outputFolder) {
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodesInCases.txt"));
        Map<String, List<String>> resultComp = new TreeMap<>(); // e.g., 0-8 -> {0-8-1, 0-8-2}
        Map<String, List<String>> comp2word = new HashMap<>();
        Map<String, Set<String>> case2word = new HashMap<>();
        for (List<String> iter: ReadFile.readString(onlyKeyFile, "\t")) {
            String[] id = iter.get(0).split("-");
            List<String> value = resultComp.getOrDefault(id[0] + "-" + id[1] , new ArrayList<>());
            value.add(iter.get(0));
            resultComp.put(id[0] + "-" + id[1], value);
            comp2word.put(iter.get(0), iter.subList(1, iter.size()));
            Set<String> words = case2word.getOrDefault(id[0] + "-" + id[1], new HashSet<>());
            words.addAll(iter.subList(1, iter.size()));
            case2word.put(id[0] + "-" + id[1], words);
        }
        System.out.println(resultComp.size());
        for (Map.Entry<String, List<String>> entry: resultComp.entrySet()) { // 每轮一个case
            String pair = entry.getKey();
            int dataset = Integer.parseInt(pair.split("-")[1]);
            int typeID = getTypeID(dataset);
            //====get triple map========
            Map<Integer, String> id2Triple = new HashMap<>();
            try {
                IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "NodeLabelIndex/" + dataset + "/")));
                for (int i = 0; i < reader.maxDoc(); i++) {
                    Document doc = reader.document(i);
                    int id = Integer.parseInt(doc.get("node"));
                    String triple = doc.get("triple");
                    id2Triple.put(id, triple);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
            //====get keyword to id map========
            Map<String, Set<Integer>> keyword2Id = new HashMap<>();
            Set<String> keywords = case2word.get(pair);
            try {
                IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LabelIdIndex/" + dataset + "/"))));
                for (String key: keywords) {
                    QueryParser parser = new QueryParser("label", new StemAnalyzer());
                    Query query = parser.parse(key);
                    TopDocs docs = searcher.search(query, 100000000);
                    ScoreDoc[] scores = docs.scoreDocs;
                    Set<Integer> ids = new HashSet<>();
                    for (ScoreDoc score: scores) {
                        int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                        ids.add(id);
                    }
                    keyword2Id.put(key, ids);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
            //====finish, get triple set========
            Set<String> result = new HashSet<>();
            boolean flag = false;
            for (String comp: entry.getValue()) {
                String subComp = comp.substring(comp.indexOf("-") + 1);
                String treePath = resultFolder1 + comp + ".txt";
                String basePath = baseFolder1 + subComp + "/subName.txt";
                File treeFile = new File(treePath);
                if (!treeFile.exists() && comp2word.get(comp).isEmpty()) {
                    treePath = resultFolder2 + subComp + ".txt";
                    basePath = baseFolder2 + subComp + "/subName.txt";
                }
                if (singleNodes.contains(subComp)) {
                    Set<String> nodes = new HashSet<>(ReadFile.readString(basePath));
                    Map<String, Boolean> covered = new HashMap<>();
                    for (String iter: comp2word.get(comp)) {
                        covered.put(iter, false);
                    }
                    for (String iter: nodes) {
                        int node = Integer.parseInt(iter);
                        String[] neighbor = id2Triple.get(node).split(",");
                        Set<Integer> pattern = new HashSet<>();
                        for (String n: neighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (node == s && p == typeID && !pattern.contains(o)) {
                                result.add(n);
                                pattern.add(o);
                            }
                            else if (node == s && p != typeID && !pattern.contains(p)){
                                result.add(n);
                                pattern.add(p);
                            }
                            else if (node == o && !pattern.contains(-p)) {
                                result.add(n);
                                pattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                    }
                }
                else {
                    File file = new File(treePath);
                    if (!file.exists()) {
                        System.out.println(treePath);
                        System.out.println("hehe");
                        flag = true;
                        break;
                    }
                    Set<String> tripleResult = tree2Triple(treePath, basePath);
                    if (tripleResult.isEmpty()) {
                        System.out.println(treePath);
                        System.out.println("fafa");
                        flag = true;
                        break;
                    }
                    Map<String, Boolean> covered = new HashMap<>();
                    for (String iter: comp2word.get(comp)) {
                        covered.put(iter, false);
                    }
                    for (String iter: tripleResult) {
                        int sid = Integer.parseInt(iter.split(" ")[0]);
                        int pid = Integer.parseInt(iter.split(" ")[1]);
                        String[] sNeighbor = id2Triple.get(sid).split(",");
                        Set<Integer> sPattern = new HashSet<>();
                        sPattern.add(pid);
                        for (String n: sNeighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (sid == s && p == typeID && !sPattern.contains(o)) {
                                result.add(n);
                                sPattern.add(o);
                            }
                            else if (sid == s && p != typeID && !sPattern.contains(p)){
                                result.add(n);
                                sPattern.add(p);
                            }
                            else if (sid == o && !sPattern.contains(-p)) {
                                result.add(n);
                                sPattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                        int oid = Integer.parseInt(iter.split(" ")[2]);
                        String[] oNeighbor = id2Triple.get(oid).split(",");
                        Set<Integer> oPattern = new HashSet<>();
                        oPattern.add(-pid);
                        for (String n: oNeighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (oid == s && p == typeID && !oPattern.contains(o)) {
                                result.add(n);
                                oPattern.add(o);
                            }
                            else if (oid == s && p != typeID && !oPattern.contains(p)) {
                                result.add(n);
                                oPattern.add(p);
                            }
                            else if (oid == o && !oPattern.contains(-p)) {
                                result.add(n);
                                oPattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                    }
                }
            }
            if (flag) {
                continue;
            }
            try (PrintWriter writer = new PrintWriter(outputFolder + pair + ".txt")) {
                for (String iter: result) {
                    writer.println(iter);
                }
                System.out.println("Finish: " + pair); /////////////////
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void getResultTriples90(String onlyKeyFile, String resultFolder1, String resultFolder2, String baseFolder1, String baseFolder2, String outputFolder) {
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodesInCases.txt"));
        Map<String, List<String>> resultComp = new TreeMap<>(); // e.g., 0-8 -> {0-8-1, 0-8-2}
        Map<String, List<String>> comp2word = new HashMap<>();
        Map<String, Set<String>> case2word = new HashMap<>();
        for (List<String> iter: ReadFile.readString(onlyKeyFile, "\t")) {
            String[] id = iter.get(0).split("-");
            List<String> value = resultComp.getOrDefault(id[0] + "-" + id[1] , new ArrayList<>());
            value.add(iter.get(0));
            resultComp.put(id[0] + "-" + id[1], value);
            comp2word.put(iter.get(0), iter.subList(1, iter.size()));
            Set<String> words = case2word.getOrDefault(id[0] + "-" + id[1], new HashSet<>());
            words.addAll(iter.subList(1, iter.size()));
            case2word.put(id[0] + "-" + id[1], words);
        }
        System.out.println(resultComp.size());
        for (Map.Entry<String, List<String>> entry: resultComp.entrySet()) { // 每轮一个case
            String pair = entry.getKey();
            int dataset = Integer.parseInt(pair.split("-")[1]);
            int typeID = getTypeID(dataset);
            //====get triple map========
            Map<Integer, String> id2Triple = new HashMap<>();
            try {
                IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "NodeLabelIndex/" + dataset + "/")));
                for (int i = 0; i < reader.maxDoc(); i++) {
                    Document doc = reader.document(i);
                    int id = Integer.parseInt(doc.get("node"));
                    String triple = doc.get("triple");
                    id2Triple.put(id, triple);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
            //====get keyword to id map========
            Map<String, Set<Integer>> keyword2Id = new HashMap<>();
            Set<String> keywords = case2word.get(pair);
            try {
                IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LabelIdIndex/" + dataset + "/"))));
                for (String key: keywords) {
                    QueryParser parser = new QueryParser("label", new StemAnalyzer());
                    Query query = parser.parse(key);
                    TopDocs docs = searcher.search(query, 100000000);
                    ScoreDoc[] scores = docs.scoreDocs;
                    Set<Integer> ids = new HashSet<>();
                    for (ScoreDoc score: scores) {
                        int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                        ids.add(id);
                    }
                    keyword2Id.put(key, ids);
                }
            }catch (Exception e) {
                e.printStackTrace();
            }
            //====finish, get triple set========
            Set<String> result = new HashSet<>();
            boolean flag = false;
            for (String comp: entry.getValue()) {
                String subComp = comp.substring(comp.indexOf("-") + 1);
                String treePath = resultFolder1 + comp + ".txt";
                String basePath = baseFolder1 + subComp + "/subName.txt";
                File treeFile = new File(treePath);
                if (!treeFile.exists() && comp2word.get(comp).isEmpty()) {
                    treePath = resultFolder2 + subComp + ".txt";
                    basePath = baseFolder2 + subComp + "/subName.txt";
                    File baseFile = new File(basePath);
                    if (!baseFile.exists()) {
                        treePath = resultFolder1 + comp + ".txt";
                        basePath = baseFolder1 + subComp + "/subName.txt";
                    }
                }
                if (singleNodes.contains(subComp)) {
                    Set<String> nodes = new HashSet<>(ReadFile.readString(basePath));
                    Map<String, Boolean> covered = new HashMap<>();
                    for (String iter: comp2word.get(comp)) {
                        covered.put(iter, false);
                    }
                    for (String iter: nodes) {
                        int node = Integer.parseInt(iter);
                        String[] neighbor = id2Triple.get(node).split(",");
                        Set<Integer> pattern = new HashSet<>();
                        for (String n: neighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (node == s && p == typeID && !pattern.contains(o)) {
                                result.add(n);
                                pattern.add(o);
                            }
                            else if (node == s && p != typeID && !pattern.contains(p)){
                                result.add(n);
                                pattern.add(p);
                            }
                            else if (node == o && !pattern.contains(-p)) {
                                result.add(n);
                                pattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                    }
                }
                else {
                    File file = new File(treePath);
                    if (!file.exists()) {
                        System.out.println(treePath);
                        System.out.println("hehe");
                        flag = true;
                        break;
                    }
                    List<String> edges = ReadFile.readString(treePath);
                    if (!edges.get(0).equals("") && !edges.get(0).contains(" ")) {
                        String singleResult = node2TripleOrNode(edges.get(0), basePath);
                        if (singleResult.contains(" ")) {
                            Map<String, Boolean> covered = new HashMap<>();
                            for (String iter: comp2word.get(comp)) {
                                covered.put(iter, false);
                            }
                            int sid = Integer.parseInt(singleResult.split(" ")[0]);
                            int pid = Integer.parseInt(singleResult.split(" ")[1]);
                            String[] sNeighbor = id2Triple.get(sid).split(",");
                            Set<Integer> sPattern = new HashSet<>();
                            sPattern.add(pid);
                            for (String n: sNeighbor) {
                                int s = Integer.parseInt(n.split(" ")[0]);
                                int p = Integer.parseInt(n.split(" ")[1]);
                                int o = Integer.parseInt(n.split(" ")[2]);
                                if (sid == s && p == typeID && !sPattern.contains(o)) {
                                    result.add(n);
                                    sPattern.add(o);
                                }
                                else if (sid == s && p != typeID && !sPattern.contains(p)){
                                    result.add(n);
                                    sPattern.add(p);
                                }
                                else if (sid == o && !sPattern.contains(-p)) {
                                    result.add(n);
                                    sPattern.add(-p);
                                }
                                for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                    Set<Integer> ids = keyword2Id.get(key.getKey());
                                    if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                        result.add(n);
                                        covered.put(key.getKey(), true);
                                    }
                                }
                            }
                            int oid = Integer.parseInt(singleResult.split(" ")[2]);
                            String[] oNeighbor = id2Triple.get(oid).split(",");
                            Set<Integer> oPattern = new HashSet<>();
                            oPattern.add(-pid);
                            for (String n: oNeighbor) {
                                int s = Integer.parseInt(n.split(" ")[0]);
                                int p = Integer.parseInt(n.split(" ")[1]);
                                int o = Integer.parseInt(n.split(" ")[2]);
                                if (oid == s && p == typeID && !oPattern.contains(o)) {
                                    result.add(n);
                                    oPattern.add(o);
                                }
                                else if (oid == s && p != typeID && !oPattern.contains(p)) {
                                    result.add(n);
                                    oPattern.add(p);
                                }
                                else if (oid == o && !oPattern.contains(-p)) {
                                    result.add(n);
                                    oPattern.add(-p);
                                }
                                for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                    Set<Integer> ids = keyword2Id.get(key.getKey());
                                    if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                        result.add(n);
                                        covered.put(key.getKey(), true);
                                    }
                                }
                            }
                        }
                        else {
                            int node = Integer.parseInt(singleResult);
                            Map<String, Boolean> covered = new HashMap<>();
                            for (String iter: comp2word.get(comp)) {
                                covered.put(iter, false);
                            }
                            String[] neighbor = id2Triple.get(node).split(",");
                            Set<Integer> pattern = new HashSet<>();
                            for (String n: neighbor) {
                                int s = Integer.parseInt(n.split(" ")[0]);
                                int p = Integer.parseInt(n.split(" ")[1]);
                                int o = Integer.parseInt(n.split(" ")[2]);
                                if (node == s && p == typeID && !pattern.contains(o)) {
                                    result.add(n);
                                    pattern.add(o);
                                }
                                else if (node == s && p != typeID && !pattern.contains(p)){
                                    result.add(n);
                                    pattern.add(p);
                                }
                                else if (node == o && !pattern.contains(-p)) {
                                    result.add(n);
                                    pattern.add(-p);
                                }
                                for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                    Set<Integer> ids = keyword2Id.get(key.getKey());
                                    if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                        result.add(n);
                                        covered.put(key.getKey(), true);
                                    }
                                }
                            }
                        }
                        continue;
                    }
                    Set<String> tripleResult = tree2Triple(treePath, basePath);
                    if (tripleResult.isEmpty()) {
                        System.out.println(treePath);
                        System.out.println("fafa");
                        flag = true;
                        break;
                    }
                    Map<String, Boolean> covered = new HashMap<>();
                    for (String iter: comp2word.get(comp)) {
                        covered.put(iter, false);
                    }
                    for (String iter: tripleResult) {
                        int sid = Integer.parseInt(iter.split(" ")[0]);
                        int pid = Integer.parseInt(iter.split(" ")[1]);
                        String[] sNeighbor = id2Triple.get(sid).split(",");
                        Set<Integer> sPattern = new HashSet<>();
                        sPattern.add(pid);
                        for (String n: sNeighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (sid == s && p == typeID && !sPattern.contains(o)) {
                                result.add(n);
                                sPattern.add(o);
                            }
                            else if (sid == s && p != typeID && !sPattern.contains(p)){
                                result.add(n);
                                sPattern.add(p);
                            }
                            else if (sid == o && !sPattern.contains(-p)) {
                                result.add(n);
                                sPattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                        int oid = Integer.parseInt(iter.split(" ")[2]);
                        String[] oNeighbor = id2Triple.get(oid).split(",");
                        Set<Integer> oPattern = new HashSet<>();
                        oPattern.add(-pid);
                        for (String n: oNeighbor) {
                            int s = Integer.parseInt(n.split(" ")[0]);
                            int p = Integer.parseInt(n.split(" ")[1]);
                            int o = Integer.parseInt(n.split(" ")[2]);
                            if (oid == s && p == typeID && !oPattern.contains(o)) {
                                result.add(n);
                                oPattern.add(o);
                            }
                            else if (oid == s && p != typeID && !oPattern.contains(p)) {
                                result.add(n);
                                oPattern.add(p);
                            }
                            else if (oid == o && !oPattern.contains(-p)) {
                                result.add(n);
                                oPattern.add(-p);
                            }
                            for (Map.Entry<String, Boolean> key: covered.entrySet()) {
                                Set<Integer> ids = keyword2Id.get(key.getKey());
                                if (!key.getValue() && (ids.contains(s) || ids.contains(p) || ids.contains(o))) {
                                    result.add(n);
                                    covered.put(key.getKey(), true);
                                }
                            }
                        }
                    }
                }
            }
            if (flag) {
                continue;
            }
            try (PrintWriter writer = new PrintWriter(outputFolder + pair + ".txt")) {
                for (String iter: result) {
                    writer.println(iter);
                }
                System.out.println("Finish: " + pair); /////////////////
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // used in getResultTriples(), read from dataset_info3
    private static int getTypeID (int dataset) {
        int typeID = 0;
        String select = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement selectStatement = connection.prepareStatement(select);
            ResultSet resultSet = selectStatement.executeQuery();
            while (resultSet.next()) {
                typeID = resultSet.getInt("type_id");
            }
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return typeID;
    }

    private static void tripleCount(String resultFolder, String outputFile) {
        Map<Integer, Set<Integer>> pairMap = new TreeMap<>();
        Map<String, Integer> countMap = new HashMap<>();
        File[] files = new File(resultFolder).listFiles();
        for (File file: files) {
            List<String> triples = ReadFile.readString(file.getPath());
            String name = file.getName().split("\\.")[0];
            countMap.put(name, triples.size());
            int pair = Integer.parseInt(name.split("-")[0]);
            int dataset = Integer.parseInt(name.split("-")[1]);
            Set<Integer> cases = pairMap.getOrDefault(pair, new TreeSet<>());
            cases.add(dataset);
            pairMap.put(pair, cases);
        }
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            for (int pair: pairMap.keySet()) {
                for (int dataset: pairMap.get(pair)) {
                    String name = pair + "-" + dataset;
                    writer.println(name + "\t" + countMap.get(name));
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void runtimeCount(String keyFile, String resultFolder, String outputFile) {
        List<List<String>> keys = ReadFile.readString(keyFile, "\t");
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            for (List<String> iter: keys) {
                String comp = iter.get(0);
                File file = new File(resultFolder + comp + ".txt");
                if (file.exists()) {
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    reader.readLine();
                    String line = reader.readLine();
                    writer.println(comp + "\t" + line);
                    reader.close();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        getResultTriples(PATHS.FileBase + "file/KeysOnlyKeywords.txt", PATHS.ProjectData + "KeyKGPWithKeywordResult/", PATHS.ProjectData + "KeyKGPResult/", PATHS.ProjectData + "KeyKGPWithKeyword/", PATHS.ProjectData + "KeyKGPNoKeyword/", PATHS.ProjectData + "SnippetWordResult/");
//        tripleCount(PATHS.ProjectData + "SnippetWordResult/", PATHS.ProjectData + "SnippetWordResultCount.txt");

//        String onlyKeyFile = PATHS.FileBase + "file/KeysOnlyKeywords80-2.txt";
//        String resultFolder1 = PATHS.ProjectData + "KeyKGPWithKeywordResult80-2/";
//        String resultFolder2 = PATHS.ProjectData + "KeyKGPResult80/";
//        String baseFolder1 = PATHS.ProjectData + "KeyKGPWithKeyword/";
//        String baseFolder2 = PATHS.ProjectData + "KeyKGPNoKeyword/";
//        String outputFolder = PATHS.ProjectData + "SnippetWordResult80-2/";

//        getResultTriples90(onlyKeyFile, resultFolder1, resultFolder2, baseFolder1, baseFolder2, outputFolder);
//        tripleCount(outputFolder, PATHS.ProjectData + "SnippetWordResultCount80-2.txt");

//    }
}
