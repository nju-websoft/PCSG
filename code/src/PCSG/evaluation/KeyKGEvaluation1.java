package PCSG.evaluation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.ReadFile;

import java.io.File;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;

public class KeyKGEvaluation1 {

    private static double edpCover(int dataset, Set<Integer> edpSet, Map<Integer, Set<Integer>> edp2prop) {
        Map<Integer, Integer> edpId2count = new HashMap<>();
        int amount = 0;
        try {
            IndexReader edpReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "EDPIndex/" + dataset + "/")));
            for (int i = 0; i < edpReader.maxDoc(); i++) {
                Document doc = edpReader.document(i);
                int id = Integer.parseInt(doc.get("id"));
                int count = Integer.parseInt(doc.get("count"));
                amount += count;
                edpId2count.put(id, count);
                Set<Integer> prop = new HashSet<>();
                String ipStr = doc.get("inProperty");
                if (ipStr.length() > 0) {
                    for (String ip: ipStr.split(" ")) {
                        prop.add(Integer.parseInt(ip));
                    }
                }
                String opStr = doc.get("outProperty");
                if (opStr.length() > 0) {
                    for (String op: opStr.split(" ")) {
                        prop.add(Integer.parseInt(op));
                    }
                }
                edp2prop.put(id, prop);
            }
            edpReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int count = 0;
        for (int edp: edpSet) {
            count += edpId2count.get(edp);
        }
        return ((double) count)/((double) amount);
    }

    private static double propCover(int dataset, Set<Integer> edpSet, Map<Integer, Set<Integer>> edp2prop, int typeId) {
        Map<Integer, Double> prop2Freq = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "FrequencyOfProperty/" + dataset + ".txt", "\t")) {
            prop2Freq.put(Integer.parseInt(iter.get(0)), Double.parseDouble(iter.get(2)));
        }
        Set<Integer> propSet = new HashSet<>();
        if (typeId != 0) {
            propSet.add(typeId);
        }
        for (int edp: edpSet) {
            propSet.addAll(edp2prop.get(edp));
        }
        double result = 0;
        for (int prop: propSet) {
            result += prop2Freq.get(prop);
        }
        return result;
    }

    private static double lpCover(int dataset, Set<String> lpSet){
        Map<String, Integer> lp2count = new HashMap<>();
        int amount = 0;
        try {
            IndexReader lpReader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LPIndex/" + dataset + "/")));
            for (int i = 0; i < lpReader.maxDoc(); i++) {
                Document doc = lpReader.document(i);
                int count = Integer.parseInt(doc.get("count"));
                amount += count;
                lp2count.put(doc.get("LP"), count);
            }
            lpReader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        int count = 0;
//        Set<String> coveredLP = new HashSet<>();
        for (String lp: lpSet) {
            count += lp2count.get(lp);
        }
        return ((double) count)/((double) amount);
    }

    public static void getEvaluationResult(String allKeyFile, String snippetFolder, String outputFile) {
        Map<Integer, Set<Integer>> order2edpSet = new HashMap<>();
        Map<Integer, Set<String>> order2lpSet = new HashMap<>();
        for (List<String> iter: ReadFile.readString(allKeyFile, "\t")) {
            int order = Integer.parseInt(iter.get(0).split("-")[0]);
            Set<Integer> edpSet = order2edpSet.getOrDefault(order, new HashSet<>());
            Set<String> lpSet = order2lpSet.getOrDefault(order, new HashSet<>());
            for (int i = 1; i < iter.size(); i++) {
                String s = iter.get(i);
                if (!s.contains("\"") && !s.contains(" ")) {
                    edpSet.add(Integer.parseInt(s));
                }
                else if (!s.contains("\"")) {
                    lpSet.add(s);
                }
                order2edpSet.put(order, edpSet);
                order2lpSet.put(order, lpSet);
            }
        }
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                List<String> iter = pairs.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                String id = i + "-" + dataset;
                File file = new File(snippetFolder + id + ".txt");
                if (!file.exists()) {
                    writer.println();
                    continue;
                }
                List<String> triples = ReadFile.readString(file.getPath());
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int pid = Integer.parseInt(t.split(" ")[1]);
                    int oid = Integer.parseInt(t.split(" ")[2]);
                    snippet.add(new Triple(sid, pid, oid));
                }
                MetricNew metric = new MetricNew(dataset);
                List<String> kws = Arrays.asList(iter.get(4).split(" "));
                double kwsCover = metric.kwsCover(snippet, kws);
                double classCover = metric.ClassCover(snippet);
                int typeId = metric.getTypeID(dataset);
                metric.close();
                Map<Integer, Set<Integer>> edp2prop = new HashMap<>();
                double edpCover = edpCover(dataset, order2edpSet.get(i), edp2prop);
                double propCover = propCover(dataset, order2edpSet.get(i), edp2prop, typeId);
                double lpCover = lpCover(dataset, order2lpSet.get(i));
                if (classCover == -1) {
                    writer.println(dataset + "\t" + kwsCover + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + kwsCover + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }
                else {
                    writer.println(dataset + "\t" + kwsCover + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + kwsCover + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }

            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntime(String snippetFolder, String resultFolder, String outputFile) {
        Set<Integer> results = new HashSet<>();
        Map<Integer, Integer> runtimeMap = new HashMap<>();
        Map<Integer, Integer> id2dataset = new HashMap<>();
        try {
            File[] resultFiles = new File(snippetFolder).listFiles();
            for (File file: resultFiles) {
                int id = Integer.parseInt(file.getName().split("\\.")[0].split("-")[0]);
                int dataset = Integer.parseInt(file.getName().split("\\.")[0].split("-")[1]);
                results.add(id);
                id2dataset.put(id, dataset);
            }
            File[] files = new File(resultFolder).listFiles();
            for (File file: files) {
                int id = Integer.parseInt(file.getName().split("\\.")[0].split("-")[0]);
                int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                int recordTime = runtimeMap.getOrDefault(id, 0);
                runtimeMap.put(id, recordTime + time);
            }
            List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                if (!results.contains(i)) {
                    writer.println();
                    continue;
                }
                writer.println(id2dataset.get(i) + "\t" + runtimeMap.getOrDefault(i, 0));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntimeAll(String keyFile, String resultFolder, String resultFolder2, String outputFile) {
        Map<Integer, Integer> runtimeMap = new HashMap<>();
        Map<Integer, Integer> id2dataset = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "SnippetWordResultCount.txt", "\t")) {
            int id = Integer.parseInt(iter.get(0).split("-")[0]);
            int dataset = Integer.parseInt(iter.get(0).split("-")[1]);
            runtimeMap.put(id, 0);
            id2dataset.put(id, dataset);
        }
        try {
            for (List<String> iter: ReadFile.readString(keyFile, "\t")) {
                String fileName = iter.get(0);
                int id = Integer.parseInt(fileName.split("-")[0]);
                File file = new File(resultFolder + fileName + ".txt");
                if (file.exists()) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    if (runtimeMap.containsKey(id)) {
                        runtimeMap.put(id, runtimeMap.get(id) + time);
                    }
                    continue;
                }
                String[] ids = iter.get(0).split("-");
                fileName = ids[1] + "-" + ids[2];
                file = new File(resultFolder2 + fileName + ".txt");
                if (!file.exists()) continue;
                if (runtimeMap.containsKey(id)) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    runtimeMap.put(id, runtimeMap.get(id) + time);
                }
            }
            List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                if (!runtimeMap.containsKey(i)) {
                    writer.println();
                    continue;
                }
                writer.println(id2dataset.get(i) + "\t" + runtimeMap.getOrDefault(i, 0));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntime90(String keyFile, String resultFolder, String resultFolder2, String outputFile) {
        Map<Integer, Integer> runtimeMap = new HashMap<>();
        Map<Integer, Integer> id2dataset = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "SnippetWordResultCount90-2.txt", "\t")) {
            int id = Integer.parseInt(iter.get(0).split("-")[0]);
            int dataset = Integer.parseInt(iter.get(0).split("-")[1]);
            runtimeMap.put(id, 0);
            id2dataset.put(id, dataset);
        }
        try {
            for (List<String> iter: ReadFile.readString(keyFile, "\t")) {
                String fileName = iter.get(0);
                int id = Integer.parseInt(fileName.split("-")[0]);
                File file = new File(resultFolder + fileName + ".txt");
                if (file.exists()) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    if (runtimeMap.containsKey(id)) {
                        runtimeMap.put(id, runtimeMap.get(id) + time);
                    }
                    continue;
                }
                String[] ids = iter.get(0).split("-");
                fileName = ids[1] + "-" + ids[2];
                file = new File(resultFolder2 + fileName + ".txt");
                if (!file.exists()) continue;
                if (runtimeMap.containsKey(id)) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    runtimeMap.put(id, runtimeMap.get(id) + time);
                }
            }
            List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                if (!runtimeMap.containsKey(i)) {
                    writer.println();
                    continue;
                }
                writer.println(id2dataset.get(i) + "\t" + runtimeMap.getOrDefault(i, 0));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntime80(String keyFile, String resultFolder, String resultFolder2, String outputFile) {
        Map<Integer, Integer> runtimeMap = new HashMap<>();
        Map<Integer, Integer> id2dataset = new HashMap<>();
        for (List<String> iter: ReadFile.readString(PATHS.ProjectData + "SnippetWordResultCount80-2.txt", "\t")) {
            int id = Integer.parseInt(iter.get(0).split("-")[0]);
            int dataset = Integer.parseInt(iter.get(0).split("-")[1]);
            runtimeMap.put(id, 0);
            id2dataset.put(id, dataset);
        }
        try {
            for (List<String> iter: ReadFile.readString(keyFile, "\t")) {
                String fileName = iter.get(0);
                int id = Integer.parseInt(fileName.split("-")[0]);
                File file = new File(resultFolder + fileName + ".txt");
                if (file.exists()) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    if (runtimeMap.containsKey(id)) {
                        runtimeMap.put(id, runtimeMap.get(id) + time);
                    }
                    continue;
                }
                String[] ids = iter.get(0).split("-");
                fileName = ids[1] + "-" + ids[2];
                file = new File(resultFolder2 + fileName + ".txt");
                if (!file.exists()) continue;
                if (runtimeMap.containsKey(id)) {
                    int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                    runtimeMap.put(id, runtimeMap.get(id) + time);
                }
            }
            List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                if (!runtimeMap.containsKey(i)) {
                    writer.println();
                    continue;
                }
                writer.println(id2dataset.get(i) + "\t" + runtimeMap.getOrDefault(i, 0));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String allKeyFile = PATHS.FileBase + "file/KeysWithKeywords.txt";
//        String snippetFolder = PATHS.ProjectData + "SnippetWordResult/";
//        String outputFile = PATHS.PaperResult + "keykg-word-score.txt";
//        getEvaluationResult(allKeyFile, snippetFolder, outputFile);
//        generalAnalyzer.transferRecords(outputFile, PATHS.PaperResult + "keykg-word-score-record.txt");
//        getRuntime(snippetFolder, PATHS.ProjectData + "KeyKGPWithKeywordResult/", PATHS.PaperResult + "keykg-word-runtime.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime.txt", PATHS.PaperResult + "keykg-word-runtime-record.txt");
//        getRuntimeAll(allKeyFile, PATHS.ProjectData + "KeyKGPWithKeywordResult/", PATHS.ProjectData + "KeyKGPResult/", PATHS.PaperResult + "keykg-word-runtime-3.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime-3.txt", PATHS.PaperResult + "keykg-word-runtime-record-3.txt");

//        String allKeyFile902 = PATHS.FileBase + "file/KeysWithKeywords90-2.txt";
//        String snippetFolder902 = PATHS.ProjectData + "SnippetWordResult90-2/";
//        String outputFile902 = PATHS.PaperResult + "keykg-word-score90-2.txt";
//        getEvaluationResult(allKeyFile902, snippetFolder902, outputFile902);
//        generalAnalyzer.transferRecords(outputFile902, PATHS.PaperResult + "keykg-word-score-record90-2.txt");
//        getRuntime(snippetFolder902, PATHS.ProjectData + "KeyKGPWithKeywordResult90-2/", PATHS.PaperResult + "keykg-word-runtime90-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime90-2.txt", PATHS.PaperResult + "keykg-word-runtime-record90-2.txt");
//        getRuntime90(allKeyFile902, PATHS.ProjectData + "KeyKGPWithKeywordResult90-2/", PATHS.ProjectData + "KeyKGPResult90/", PATHS.PaperResult + "keykg-word-runtime90-3.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime90-3.txt", PATHS.PaperResult + "keykg-word-runtime-record90-3.txt");

//        String allKeyFile802 = PATHS.FileBase + "file/KeysWithKeywords80-2.txt";
//        String snippetFolder802 = PATHS.ProjectData + "SnippetWordResult80-2/";
//        String outputFile802 = PATHS.PaperResult + "keykg-word-score80-2.txt";
//        getEvaluationResult(allKeyFile802, snippetFolder802, outputFile802);
//        generalAnalyzer.transferRecords(outputFile802, PATHS.PaperResult + "keykg-word-score-record80-2.txt");
//        getRuntime(snippetFolder802, PATHS.ProjectData + "KeyKGPWithKeywordResult80-2/", PATHS.PaperResult + "keykg-word-runtime80-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime80-2.txt", PATHS.PaperResult + "keykg-word-runtime-record80-2.txt");
//        getRuntime80(allKeyFile802, PATHS.ProjectData + "KeyKGPWithKeywordResult80-2/", PATHS.ProjectData + "KeyKGPResult80/", PATHS.PaperResult + "keykg-word-runtime80-3.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "keykg-word-runtime80-3.txt", PATHS.PaperResult + "keykg-word-runtime-record80-3.txt");
//    }

}
