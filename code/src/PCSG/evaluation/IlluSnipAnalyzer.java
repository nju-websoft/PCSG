package PCSG.evaluation;

import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IlluSnipAnalyzer {

    public static void getEvaluationResult(String listFile, String snippetFolder, String outputFile) {
        List<List<Integer>> datasets = ReadFile.readInteger(listFile, "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: datasets) {
                int dataset = iter.get(0);
                String snippetStr = ReadFile.readString(snippetFolder + dataset + ".txt").get(0);
                String[] triples = snippetStr.substring(snippetStr.indexOf(";") + 1).split(",");
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int oid = Integer.parseInt(t.split(" ")[1]);
                    int pid = Integer.parseInt(t.split(" ")[2]);
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

    private static void getRuntime(String listFile, String snippetFolder, String outputFile) {
        List<List<Integer>> datasets = ReadFile.readInteger(listFile, "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: datasets) {
                int dataset = iter.get(0);
                int runtime = Integer.parseInt(ReadFile.readString(snippetFolder + dataset + ".txt").get(1));
                writer.println(dataset + "\t" + runtime);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getBaseEvaluationResult(String snippetFolder, String outputFile) {
        List<Integer> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0);
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int dataset: datasets) {
                String snippetStr = ReadFile.readString(snippetFolder + dataset + ".txt").get(0);
                String[] triples = snippetStr.substring(snippetStr.indexOf(";") + 1).split(",");
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int oid = Integer.parseInt(t.split(" ")[1]);
                    int pid = Integer.parseInt(t.split(" ")[2]);
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

    private static void getBaseRuntime(String snippetFolder, String outputFile) {
        List<Integer> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0);
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int dataset: datasets) {
                int runtime = Integer.parseInt(ReadFile.readString(snippetFolder + dataset + ".txt").get(1));
                writer.println(dataset + "\t" + runtime);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String listFile = PATHS.ProjectData + "SnippetResultCount.txt";
//        String snippetFolder = PATHS.ProjectData + "IlluSnipResult/";
//        String outputFile = PATHS.PaperResult + "IlluSnip-score.txt";
//        getEvaluationResult(listFile, snippetFolder, outputFile);
//        getRuntime(listFile, snippetFolder, PATHS.PaperResult + "IlluSnip-runtime.txt");

//        String listFile90 = PATHS.ProjectData + "SnippetResultCount90.txt";
//        String snippetFolder90 = PATHS.ProjectData + "IlluSnipResult90/";
//        String outputFile90 = PATHS.PaperResult + "IlluSnip-score90.txt";
//        getEvaluationResult(listFile90, snippetFolder90, outputFile90);
//        getRuntime(listFile90, snippetFolder90, PATHS.PaperResult + "IlluSnip-runtime90.txt");

//        String listFile80 = PATHS.ProjectData + "SnippetResultCount80.txt";
//        String snippetFolder80 = PATHS.ProjectData + "IlluSnipResult80/";
//        String outputFile80 = PATHS.PaperResult + "IlluSnip-score80.txt";
//        getEvaluationResult(listFile80, snippetFolder80, outputFile80);
//        getRuntime(listFile80, snippetFolder80, PATHS.PaperResult + "IlluSnip-runtime80.txt");

//    }
}
