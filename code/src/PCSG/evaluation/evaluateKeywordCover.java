package PCSG.evaluation;

import PCSG.PATHS;
import PCSG.beans.Triple;
import PCSG.util.ReadFile;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class evaluateKeywordCover {

    public static void getEvaluationResult(String snippetFolder, String outputFile) {
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int i = 0; i < pairs.size(); i++) {
                List<String> iter = pairs.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                File file = new File(snippetFolder + i + "-" + dataset + ".txt");
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
                MetricsKeyword metric = new MetricsKeyword(dataset);
                List<String> kws = Arrays.asList(iter.get(2).split(" "));// !!
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
//        String snippetFolder = PATHS.ProjectData + "SnippetWordResult/";
//        String outputFile = PATHS.PaperResult + "keykg-word-score-keyword.txt";
//        getEvaluationResult(snippetFolder, outputFile);
//        generalAnalyzer.transferRecords(outputFile, PATHS.PaperResult + "keykg-word-score-keyword-record.txt");

//        String snippetFolder902 = PATHS.ProjectData + "SnippetWordResult90-2/";
//        String outputFile902 = PATHS.PaperResult + "keykg-word-score-keyword90-2.txt";
//        getEvaluationResult(snippetFolder902, outputFile902);
//        generalAnalyzer.transferRecords(outputFile902, PATHS.PaperResult + "keykg-word-score-keyword-record90-2.txt");
//
//        String snippetFolder802 = PATHS.ProjectData + "SnippetWordResult80-2/";
//        String outputFile802 = PATHS.PaperResult + "keykg-word-score-keyword80-2.txt";
//        getEvaluationResult(snippetFolder802, outputFile802);
//        generalAnalyzer.transferRecords(outputFile802, PATHS.PaperResult + "keykg-word-score-keyword-record80-2.txt");
//    }
}
