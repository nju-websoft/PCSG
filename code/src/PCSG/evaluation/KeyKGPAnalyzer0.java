package PCSG.evaluation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.util.DBUtil;
import PCSG.util.IndexUtil;
import PCSG.util.ReadFile;

import java.io.*;
import java.nio.file.Paths;
import java.sql.*;
import java.util.*;

public class KeyKGPAnalyzer0 {

    //Count #Set Cover Components -- getSetCoverComponents(int dataset)
    private static void countSCPResults() {
        String componentPath = "D:/Work/ISWC2021Index/ComponentIndex/";
        String SCPResultPath = "D:/Work/ISWC2021Index/SCPResult/";
        Connection connection = new DBUtil().conn;
        String select = "select distinct dataset_local_id from dataset_info_202007 where triple_count > 0 order by dataset_local_id";
        try {
            PreparedStatement selectStatement = connection.prepareStatement(select);
            ResultSet resultSet = selectStatement.executeQuery();
            BufferedWriter writer = new BufferedWriter(new FileWriter(new File("count.txt")));
            while (resultSet.next()) {
                int dataset = resultSet.getInt("dataset_local_id");
                int compAmount = IndexUtil.countDocuments(componentPath + dataset + "/");
                File file = new File(SCPResultPath + dataset + ".txt");
                if (file.exists()) {
                    BufferedReader reader = new BufferedReader(new FileReader(file));
                    String line = reader.readLine();
                    int count = line.split(" ").length;
                    reader.close();
                    writer.write(dataset + "\t" + compAmount + "\t" + count + "\n");
                }
                else {
                    writer.write(dataset + "\t" + compAmount + "\n");
                }
                System.out.println(dataset);
            }
            writer.close();
        } catch (SQLException | IOException e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

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

    private static void getResultTriples(String keyFile, String resultFolder, String baseFolder, String outputFolder) {
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodes.txt"));
        Map<Integer, List<String>> resultComp = new TreeMap<>();
        for (List<String> iter: ReadFile.readString(keyFile, "\t")) {
            int dataset = Integer.parseInt(iter.get(0).split("-")[0]);
            List<String> value = resultComp.getOrDefault(dataset, new ArrayList<>());
            value.add(iter.get(0));
            resultComp.put(dataset, value);
        }
        for (Map.Entry<Integer, List<String>> entry: resultComp.entrySet()) {
            int dataset = entry.getKey();
            boolean flag = false;
            Set<String> triples = new HashSet<>();
            Set<String> nodes = new HashSet<>();
            for (String comp: entry.getValue()) {
                String treePath = resultFolder + comp + ".txt";
                String basePath = baseFolder + comp + "/subName.txt";
                if (singleNodes.contains(comp)) {
                    nodes.addAll(ReadFile.readString(basePath));
                }
                else {
                    Set<String> tripleResult = tree2Triple(treePath, basePath);
                    if (tripleResult.isEmpty()) {
                        flag = true;
                        break;
                    }
                    triples.addAll(tripleResult);
                }
            }
            if (flag) {
                continue;
            }
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
            //====finish, get triple set========
            int typeID = getTypeID(dataset);
            Set<String> result = new HashSet<>(triples);
            for (String iter: triples) {
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
                }
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
                }
            }
            //====finish, output========
            try (PrintWriter writer = new PrintWriter(outputFolder + dataset + ".txt")) {
                for (String iter: result) {
                    writer.println(iter);
                }
//                for (String iter: singleNodes) {
//                    writer.println(iter);
//                }
                System.out.println("Finish: " + dataset); /////////////////
            }catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    // NOTE the result tree may have only one node but no egdes
    private static void getResultTriples90(String keyFile, String resultFolder, String baseFolder, String outputFolder) {
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodes.txt"));
        Map<Integer, List<String>> resultComp = new TreeMap<>();
        for (List<String> iter: ReadFile.readString(keyFile, "\t")) {
            int dataset = Integer.parseInt(iter.get(0).split("-")[0]);
            List<String> value = resultComp.getOrDefault(dataset, new ArrayList<>());
            value.add(iter.get(0));
            resultComp.put(dataset, value);
        }
        for (Map.Entry<Integer, List<String>> entry: resultComp.entrySet()) {
            int dataset = entry.getKey();
            boolean flag = false;
            Set<String> triples = new HashSet<>();
            Set<String> nodes = new HashSet<>();
            for (String comp: entry.getValue()) {
                String treePath = resultFolder + comp + ".txt";
                String basePath = baseFolder + comp + "/subName.txt";
                if (singleNodes.contains(comp)) {
                    nodes.addAll(ReadFile.readString(basePath));
                }
                else {
                    List<String> edges = ReadFile.readString(treePath);
                    if (!edges.get(0).equals("") && !edges.get(0).contains(" ")) {
                        String singleResult = node2TripleOrNode(edges.get(0), basePath);
                        if (singleResult.contains(" ")) {
                            triples.add(singleResult);
                        }
                        else {
                            nodes.add(singleResult);
                        }
//                        break;
                        continue;
                    }
                    Set<String> tripleResult = tree2Triple(treePath, basePath);
                    if (tripleResult.isEmpty()) {
                        flag = true;
                        break;
                    }
                    triples.addAll(tripleResult);
                }
            }
            if (flag) {
                continue;
            }
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
            //====finish, get triple set========
            int typeID = getTypeID(dataset);
            Set<String> result = new HashSet<>(triples);
            for (String iter: triples) {
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
                }
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
                }
            }
            //====finish, output========
            try (PrintWriter writer = new PrintWriter(outputFolder + dataset + ".txt")) {
                for (String iter: result) {
                    writer.println(iter);
                }
//                for (String iter: singleNodes) {
//                    writer.println(iter);
//                }
                System.out.println("Finish: " + dataset); /////////////////
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
            connection.close(); // 一定要显式close，不要等java VM来处理垃圾
        }catch (Exception e){
            e.printStackTrace();
        }
        return typeID;
    }

    private static void tripleCount(String resultFolder, String outputFile) {
        Map<Integer, Integer> countMap = new TreeMap<>();
        try (PrintWriter writer = new PrintWriter(outputFile)) {
            File[] files = new File(resultFolder).listFiles();
            for (File iter: files) {
                List<String> triples = ReadFile.readString(iter.getPath());
                int dataset = Integer.parseInt(iter.getName().split("\\.")[0]);
                countMap.put(dataset, triples.size());
            }
            for (int iter: countMap.keySet()) {
                writer.println(iter + "\t" + countMap.get(iter));
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
//        getResultTriples(PATHS.FileBase + "file/Keys.txt", PATHS.ProjectData + "KeyKGPResult/", PATHS.ProjectData + "KeyKGPNoKeyword/", PATHS.ProjectData + "SnippetResult/");
//        tripleCount(PATHS.ProjectData + "SnippetResult/", PATHS.ProjectData + "SnippetResultCount.txt");

//        getResultTriples90(PATHS.FileBase + "file/Keys90.txt", PATHS.ProjectData + "KeyKGPResult90/", PATHS.ProjectData + "KeyKGPNoKeyword/", PATHS.ProjectData + "SnippetResult901/");
//        tripleCount(PATHS.ProjectData + "SnippetResult901/", PATHS.ProjectData + "SnippetResultCount901.txt");

//        getResultTriples90(PATHS.FileBase + "file/Keys80.txt", PATHS.ProjectData + "KeyKGPResult80/", PATHS.ProjectData + "KeyKGPNoKeyword/", PATHS.ProjectData + "SnippetResult80/");
//        tripleCount(PATHS.ProjectData + "SnippetResult80/", PATHS.ProjectData + "SnippetResultCount80.txt");
//    }
}
