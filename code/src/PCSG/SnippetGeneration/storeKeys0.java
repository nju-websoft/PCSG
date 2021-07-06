package PCSG.SnippetGeneration;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.util.IndexUtil;
import PCSG.util.ReadFile;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class storeKeys0 { //NO KEYWORD!!

    // record all isolated nodes in run cases
    private static void recordSingleNodes() {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/Keys.txt", "\t");
        try (PrintWriter writer = new PrintWriter(PATHS.FileBase + "file/IsolatedNodes.txt")) {
            for (List<String> iter : pair) {
                String id = iter.get(0);
                IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "KeyKGPNoKeyword/" + id + "/subName/")));
                if (reader.maxDoc() == 1) {
                    writer.write(id + "\n");
                }
                reader.close();
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    // used in processAll(), each item in ``result'' is all edp&lp in the corresponding component
    private static List<String> getKeysNoWords(int dataset) throws IOException {
        List<String> result = new ArrayList<>();
        File file = new File(PATHS.ProjectData + "SCPResult/" + dataset + ".txt");
        if (!file.exists()) {
            return result;
        }
        Map<Integer, String> id2lp = new HashMap<>();
        IndexReader reader = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LPIndex/" + dataset + "/")));
        for (int i = 0; i < reader.maxDoc(); i++) {
            Document doc = reader.document(i);
            id2lp.put(Integer.parseInt(doc.get("id")), doc.get("LP"));
        }
        reader.close();
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResult/" + dataset + ".txt", " ");
        for (int comp: comps.get(0)) {
            StringBuilder currentStr = new StringBuilder(dataset + "-" + comp + "\t");
            String edpStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "edp");
            edpStr = edpStr.replace(" ", "\t");
            currentStr.append(edpStr.trim()).append("\t");
            String lpIdStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "lp");
            if (lpIdStr.length() > 0) {
                for (String lp: lpIdStr.split(" ")) {
                    currentStr.append(id2lp.get(Integer.parseInt(lp))).append("\t");
                }
            }
            result.add(currentStr.toString().trim());
        }
        return result;
    }

    // get all edps and lps as keys, stored in file/Keys.txt
    public static void processAll(int start, int end) {
        List<List<Integer>> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t");
        try (PrintWriter writer = new PrintWriter(PATHS.FileBase + "file/Keys.txt")) {
            for (int dataset: datasets.get(0)) {
                if (dataset < start || dataset > end) {
                    continue;
                }
                List<String> result = getKeysNoWords(dataset);
                if (!result.isEmpty()) {
                    for (String iter: result) {
                        writer.println(iter);
                    }
                    System.out.println("Finish dataset " + dataset + ". ");
                }
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        processAll(0, 9630);
//    }

}
