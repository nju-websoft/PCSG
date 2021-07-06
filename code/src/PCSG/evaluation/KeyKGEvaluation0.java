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

public class KeyKGEvaluation0 {

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

    public static void getEvaluationResult(String listFile, String snippetFolder, String outputFile) {
        List<List<Integer>> datasets = ReadFile.readInteger(listFile, "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: datasets) {
                int dataset = iter.get(0);
                List<String> triples = ReadFile.readString(snippetFolder + dataset + ".txt");
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int pid = Integer.parseInt(t.split(" ")[1]);
                    int oid = Integer.parseInt(t.split(" ")[2]);
                    snippet.add(new Triple(sid, pid, oid));
                }
                MetricNew metric = new MetricNew(dataset);
                double classCover = metric.ClassCover(snippet);
                double propCover = metric.PropCover(snippet);
                double edpCover = metric.edpCover(snippet);
                double lpCover = metric.lpCover(snippet);
                if (classCover == -1) {
                    writer.println(dataset + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }
                else {
                    writer.println(dataset + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }
                metric.close();
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntime(String listFile, String resultFolder, String outputFile) {
        List<List<Integer>> datasets = ReadFile.readInteger(listFile, "\t");
        Map<Integer, Integer> runtimeMap = new HashMap<>();
        try {
            File[] files = new File(resultFolder).listFiles();
            for (File file: files) {
                int dataset = Integer.parseInt(file.getName().split("\\.")[0].split("-")[0]);
                int time = Integer.parseInt(ReadFile.readString(file.getPath()).get(1));
                int recordTime = runtimeMap.getOrDefault(dataset, 0);
                runtimeMap.put(dataset, recordTime + time);
            }
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: datasets) {
                int dataset = iter.get(0);
                writer.println(dataset + "\t" + runtimeMap.getOrDefault(dataset, 0));
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String listFile = PATHS.ProjectData + "SnippetResultCount.txt";
//        String snippetFolder = PATHS.ProjectData + "SnippetResult/";
//        String outputFile = PATHS.PaperResult + "keykg-score.txt";
//        getEvaluationResult(listFile, snippetFolder, outputFile);
//        getRuntime(listFile, PATHS.ProjectData + "KeyKGPResult/", PATHS.PaperResult + "keykg-runtime.txt");

//        String listFile90 = PATHS.ProjectData + "SnippetResultCount90.txt";
//        String snippetFolder90 = PATHS.ProjectData + "SnippetResult90/";
//        String outputFile90 = PATHS.PaperResult + "keykg-score90.txt";
//        getEvaluationResult(listFile90, snippetFolder90, outputFile90);
//        getRuntime(listFile, PATHS.ProjectData + "KeyKGPResult90/", PATHS.PaperResult + "keykg-runtime90.txt");

//        String listFile80 = PATHS.ProjectData + "SnippetResultCount80.txt";
//        String snippetFolder80 = PATHS.ProjectData + "SnippetResult80/";
//        String outputFile80 = PATHS.PaperResult + "keykg-score80.txt";
//        getEvaluationResult(listFile80, snippetFolder80, outputFile80);
//        getRuntime(listFile, PATHS.ProjectData + "KeyKGPResult80/", PATHS.PaperResult + "keykg-runtime80.txt");
//    }
}
