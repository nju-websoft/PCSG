package PCSG.evaluation;

import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.ReadFile;

import java.io.File;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class KSDAnalyzer {

    private static void getEvaluationResult(String resultFolder, String outputFile) {
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                List<String> iter = pairs.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                List<String> kws = Arrays.asList(iter.get(4).split(" "));
                File file = new File(resultFolder + i + "-" + dataset + ".txt");
                if (!file.exists()) {
                    writer.println();
                    continue;
                }
                String snippetStr = ReadFile.readString(file.getPath()).get(0);
                String[] triples = snippetStr.substring(snippetStr.indexOf(";") + 1).split(",");
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int oid = Integer.parseInt(t.split(" ")[1]);
                    int pid = Integer.parseInt(t.split(" ")[2]);
                    snippet.add(new Triple(sid, pid, oid));
                }
                MetricNew metric = new MetricNew(dataset);
                double kwsCover = metric.kwsCover(snippet, kws);
                double classCover = metric.ClassCover(snippet);
                double propCover = metric.PropCover(snippet);
                double edpCover = metric.edpCover(snippet);
                double lpCover = metric.lpCover(snippet);
                if (classCover == -1) {
                    writer.println(dataset + "\t" + kwsCover + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + kwsCover + "\t" + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }
                else {
                    writer.println(dataset + "\t" + kwsCover + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                    System.out.println(dataset + "\t" + kwsCover + "\t" + classCover + "\t" + propCover + "\t" + edpCover + "\t" + lpCover);
                }
                metric.close();
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getRuntime(String resultFolder, String outputFile) {
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                List<String> iter = pairs.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                File file = new File(resultFolder + i + "-" + dataset + ".txt");
                if (!file.exists()) {
                    writer.println();
                    continue;
                }
                int runtime = Integer.parseInt(ReadFile.readString(file.getPath()).get(2));
                writer.println(i + "\t" + dataset + "\t" + runtime);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void getEvaluationResultKeyword(String resultFolder, String outputFile) {
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                List<String> iter = pairs.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                File file = new File(resultFolder + i + "-" + dataset + ".txt");
                if (!file.exists()) {
                    writer.println();
                    continue;
                }
                String snippetStr = ReadFile.readString(file.getPath()).get(0);
                String[] triples = snippetStr.substring(snippetStr.indexOf(";") + 1).split(",");
                Set<Triple> snippet = new HashSet<>();
                for (String t: triples) {
                    int sid = Integer.parseInt(t.split(" ")[0]);
                    int oid = Integer.parseInt(t.split(" ")[1]);
                    int pid = Integer.parseInt(t.split(" ")[2]);
                    snippet.add(new Triple(sid, pid, oid));
                }
                MetricsKeyword metric = new MetricsKeyword(dataset);
                List<String> kws = Arrays.asList(iter.get(2).split(" ")); // !!
                double coKyw = metric.coKyw(snippet, kws);
                double coCnx = metric.coCnx(snippet, kws);
                metric.close();
                writer.println(dataset + "\t" + coKyw + "\t" + coCnx);
                System.out.println(dataset + "\t" + coKyw + "\t" + coCnx);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {

//        String resultFolder = PATHS.ProjectData + "KSDResult/";
//        String outputFile = PATHS.PaperResult + "ksd-score.txt";
//        getEvaluationResult(resultFolder, outputFile);
//        generalAnalyzer.transferRecords(outputFile, PATHS.PaperResult + "ksd-score-record.txt");
//        getRuntime(resultFolder, PATHS.PaperResult + "ksd-runtime.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-runtime.txt", PATHS.PaperResult + "ksd-runtime-record.txt");
//        getEvaluationResultKeyword(resultFolder, PATHS.PaperResult + "ksd-score-keyword2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-score-keyword.txt", PATHS.PaperResult + "ksd-score-keyword-record.txt");

//        String resultFolder902 = PATHS.ProjectData + "KSDResult90-2/";
//        String outputFile902 = PATHS.PaperResult + "ksd-score90-2.txt";
//        getEvaluationResult(resultFolder902, outputFile902);
//        generalAnalyzer.transferRecords(outputFile902, PATHS.PaperResult + "ksd-score-record90-2.txt");
//        getRuntime(resultFolder902, PATHS.PaperResult + "ksd-runtime90-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-runtime90-2.txt", PATHS.PaperResult + "ksd-runtime-record90-2.txt");
//        getEvaluationResultKeyword(resultFolder902, PATHS.PaperResult + "ksd-score-keyword90-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-score-keyword90-2.txt", PATHS.PaperResult + "ksd-score-keyword-record90-2.txt");

//        String resultFolder802 = PATHS.ProjectData + "KSDResult80-2/";
//        String outputFile802 = PATHS.PaperResult + "ksd-score80-2.txt";
//        getEvaluationResult(resultFolder802, outputFile802);
//        generalAnalyzer.transferRecords(outputFile802, PATHS.PaperResult + "ksd-score-record80-2.txt");
//        getRuntime(resultFolder802, PATHS.PaperResult + "ksd-runtime80-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-runtime80-2.txt", PATHS.PaperResult + "ksd-runtime-record80-2.txt");
//        getEvaluationResultKeyword(resultFolder802, PATHS.PaperResult + "ksd-score-keyword80-2.txt");
//        generalAnalyzer.transferRecords(PATHS.PaperResult + "ksd-score-keyword80-2.txt", PATHS.PaperResult + "ksd-score-keyword-record80-2.txt");

//    }
}
