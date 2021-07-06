package PCSG.SnippetGeneration;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.util.IndexUtil;
import PCSG.util.ReadFile;
import PCSG.util.StemAnalyzer;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;

public class storeKeys1 {

    // record all isolated nodes in run cases
    public static void recordSingleNodesInCases() {
        List<List<String>> pair = ReadFile.readString(PATHS.ProjectData + "file/CasesWithKeywords.txt", "\t");
        try (PrintWriter writer = new PrintWriter(new FileWriter(PATHS.ProjectData + "file/IsolatedNodesInCases.txt"))) {
            for (List<String> iter : pair) {
                String id = iter.get(0);
                List<String> nodes = ReadFile.readString(PATHS.ProjectData + "KeyKGPWithKeyword/" + id + "/subName.txt");
                if (nodes.size() == 1) {
                    writer.println(id);
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    // pair: which row in QueryPair.txt, start from 0
    private static List<String> getKeysAll(int pair, int dataset, List<String> keywords) throws IOException, ParseException {
        List<String> result = new ArrayList<>();
        File file = new File(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt");
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
        String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
        Map<String, Set<Integer>> keyword2Component = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
        for (String iter: keywords) {
            QueryParser parser = new QueryParser("text", new StemAnalyzer());
            Query query = parser.parse(iter);
            TopDocs docs = searcher.search(query, 100000000);
            ScoreDoc[] scores = docs.scoreDocs;
            Set<Integer> compValue = new HashSet<>();
            for (ScoreDoc score: scores) {
                int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                compValue.add(id);
            }
            keyword2Component.put(iter, compValue);
        }
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt", " ");
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
            for (Map.Entry<String, Set<Integer>> iter: keyword2Component.entrySet()) {
                if (iter.getValue().contains(comp)) {
                    currentStr.append("\"").append(iter.getKey()).append("\"").append("\t");
                }
            }
            result.add(currentStr.toString().trim());
        }
        return result;
    }

    // record only keywords but not patterns in each component
    private static List<String> getKeywords(int pair, int dataset, List<String> keywords) throws IOException, ParseException {
        List<String> result = new ArrayList<>();
        File file = new File(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt");
        if (!file.exists()) {
            System.out.println(dataset + "-" + pair + ": NO FILE!! ");
            return result;
        }
        String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
        Map<String, Set<Integer>> keyword2Component = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
        for (String iter: keywords) {
            QueryParser parser = new QueryParser("text", new StemAnalyzer());
            Query query = parser.parse(iter);
            TopDocs docs = searcher.search(query, 100000000);
            ScoreDoc[] scores = docs.scoreDocs;
            Set<Integer> compValue = new HashSet<>();
            for (ScoreDoc score: scores) {
                int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                compValue.add(id);
            }
            keyword2Component.put(iter, compValue);
        }
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt", " ");
        for (int comp: comps.get(0)) { // for each component
            StringBuilder currentStr = new StringBuilder(dataset + "-" + comp + "\t");
            for (Map.Entry<String, Set<Integer>> iter: keyword2Component.entrySet()) {
                if (iter.getValue().contains(comp)) {
                    currentStr.append(iter.getKey()).append("\t");
                }
            }
            result.add(currentStr.toString().trim());
        }
        return result;
    }

    // generate KeysWithKeywords.txt and/or KeysOnlyKeywords.txt
    public static void processAll(int start, int end) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try (PrintWriter writer = new PrintWriter(PATHS.FileBase + "file/KeysWithKeywords.txt")) { // PATHS.FileBase + "file/KeysOnlyKeywords.txt"
            for (int i = 0; i < pair.size(); i++) {
                List<String> iter = pair.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                if (dataset < start || dataset > end) {
                    continue;
                }
                List<String> keywords = new ArrayList<>(Arrays.asList(iter.get(4).split(" ")));
                List<String> result = getKeysAll(i, dataset, keywords);
//                List<String> result = getKeywords(i, dataset, keywords);
                if (!result.isEmpty()) {
                    for (String s: result) {
                        writer.println(i + "-" + s);
                    }
                    System.out.println("Finish pair " + i + "-" + dataset + ". ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // record all words appeared in all query cases for each component (decided by query pairs)
    private static void recordUsedWordsInAllCases() {
        Map<Integer, Set<Integer>> runCases = new TreeMap<>(); // dataset -> set of components
        Map<String, Set<String>> cases2Words = new HashMap<>(); // dataset-component -> set of words
        try (BufferedReader reader = new BufferedReader(new FileReader(PATHS.ProjectData + "file/KeysOnlyKeywords.txt"))){
            String line = null;
            while ((line = reader.readLine()) != null) {
                String[] ids = line.split("\t")[0].split("-");
                int dataset = Integer.parseInt(ids[1]);
                int component = Integer.parseInt(ids[2]);
                Set<Integer> temp = runCases.getOrDefault(dataset, new TreeSet<>());
                temp.add(component);
                runCases.put(dataset, temp);
                if (line.contains("\t")) {
                    String id = dataset + "-" + component;
                    Set<String> keywords = cases2Words.getOrDefault(id, new TreeSet<>());
                    String[] words = line.split("\t");
                    keywords.addAll(Arrays.asList(words).subList(1, words.length));
                    cases2Words.put(id, keywords);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        try (PrintWriter writer = new PrintWriter(new FileWriter(PATHS.ProjectData + "file/CasesWithKeywords.txt"))) {
            for (Map.Entry<Integer, Set<Integer>> iter: runCases.entrySet()) {
                int dataset = iter.getKey();
                for (int valIter: iter.getValue()) {
                    String id = dataset + "-" + valIter;
                    writer.print(id);
                    if (cases2Words.containsKey(id)) {
                        for (String wordIter: cases2Words.get(id)) {
                            writer.print("\t" + wordIter);
                        }
                    }
                    writer.println();
                }
            }
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void getKeysAll(int pair, int dataset, List<String> keywords, double percent, List<String> allKey, List<String> onlyKey) throws Exception {
        File file = new File(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt");
        if (!file.exists()) {
            return;
        }
        Map<Integer, String> lpid2lp = new HashMap<>();
        Map<Integer, Integer> lpid2count = new HashMap<>();
        int lpCount = 0;
        IndexReader readerlp = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LPIndex/" + dataset + "/")));
        for (int i = 0; i < readerlp.maxDoc(); i++) {
            Document doc = readerlp.document(i);
            int lpid = Integer.parseInt(doc.get("id"));
            lpid2lp.put(lpid, doc.get("LP"));
            int count = Integer.parseInt(doc.get("count"));
            lpid2count.put(lpid, count);
            lpCount += count;
        }
        readerlp.close();
        Map<Integer, Integer> edp2count = new HashMap<>();
        int entityCount = 0;
        IndexReader readeredp = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "EDPIndex/" + dataset + "/")));
        for (int i = 0; i < readeredp.maxDoc(); i++) {
            Document doc = readeredp.document(i);
            int count = Integer.parseInt(doc.get("count"));
            edp2count.put(Integer.parseInt(doc.get("id")), count);
            entityCount += count;
        }
        readeredp.close();
        String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
        Map<String, Set<Integer>> keyword2Component = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
        for (String iter: keywords) {
            QueryParser parser = new QueryParser("text", new StemAnalyzer());
            Query query = parser.parse(iter);
            TopDocs docs = searcher.search(query, 100000000);
            ScoreDoc[] scores = docs.scoreDocs;
            Set<Integer> compValue = new HashSet<>();
            for (ScoreDoc score: scores) {
                int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                compValue.add(id);
            }
            keyword2Component.put(iter, compValue);
        }
        double edpNum = entityCount * percent;
        double lpNum = lpCount * percent;
        double wordNum = keywords.size() * percent;
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt", " ");
        double currentEdp = 0;
        Set<Integer> existingEdp = new HashSet<>();
        double currentLp = 0;
        Set<Integer> existingLp = new HashSet<>();
        double currentWord = 0;
        Set<String> existingWord = new HashSet<>();
        for (int comp: comps.get(0)) {
            StringBuilder currentStr = new StringBuilder(dataset + "-" + comp + "\t");
            StringBuilder currentStrWord = new StringBuilder(dataset + "-" + comp + "\t");
            String[] edpStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "edp").split(" ");
            for (String edp: edpStr) {
                int edpId = Integer.parseInt(edp);
                if (currentEdp < edpNum && !existingEdp.contains(edpId)) {
                    currentStr.append(edp).append("\t");
                    currentEdp += edp2count.get(edpId);
                    existingEdp.add(edpId);
                }
            }
            String lpIdStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "lp");
            if (lpIdStr.length() > 0) {
                for (String lp: lpIdStr.split(" ")) {
                    int lpId = Integer.parseInt(lp);
                    if (currentLp < lpNum && !existingLp.contains(lpId)) {
                        currentStr.append(lpid2lp.get(lpId)).append("\t");
                        currentLp += lpid2count.get(lpId);
                        existingLp.add(lpId);
                    }
                }
            }
            for (Map.Entry<String, Set<Integer>> iter: keyword2Component.entrySet()) {
                String word = iter.getKey();
                if (currentWord < wordNum && !existingWord.contains(word) && iter.getValue().contains(comp)) {
                    currentStr.append("\"").append(word).append("\"").append("\t");
                    currentStrWord.append(word).append("\t");
                    currentWord += 1;
                    existingWord.add(word);
                }
            }
            String line = currentStr.toString().trim();
            if (line.contains("\t")) {
                allKey.add(line);
                onlyKey.add(currentStrWord.toString().trim());
            }
        }
    }

    // top percent keys
    private static void processAll(int start, int end, double percent) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try (PrintWriter writer1 = new PrintWriter(PATHS.FileBase + "file/KeysWithKeywords" + ((int)(percent*100)) + ".txt"); //e.g., file/KeysWithKeywords90.txt
             PrintWriter writer2 = new PrintWriter(PATHS.FileBase + "file/KeysOnlyKeywords" + ((int)(percent*100)) + ".txt")) { // e.g., file/KeysOnlyKeywords90.txt
            for (int i = 0; i < pair.size(); i++) {
                List<String> iter = pair.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                if (dataset < start || dataset > end) {
                    continue;
                }
                List<String> keywords = new ArrayList<>(Arrays.asList(iter.get(4).split(" ")));
                List<String> allKey = new ArrayList<>();
                List<String> onlyKey = new ArrayList<>();
                getKeysAll(i, dataset, keywords, percent, allKey, onlyKey);
                if (!allKey.isEmpty()) {
                    for (String s: allKey) {
                        writer1.println(i + "-" + s);
                    }
                    for (String s: onlyKey) {
                        writer2.println(i + "-" + s);
                    }
                    System.out.println("Finish pair " + i + "-" + dataset + ". ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getKeysAllAdd(int pair, int dataset, List<String> keywords, double percent, List<String> allKey, List<String> onlyKey) throws Exception {
        File file = new File(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt");
        if (!file.exists()) {
            return;
        }
        Map<Integer, String> lpid2lp = new HashMap<>();
        Map<Integer, Integer> lpid2count = new HashMap<>();
        int lpCount = 0;
        IndexReader readerlp = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "LPIndex/" + dataset + "/")));
        for (int i = 0; i < readerlp.maxDoc(); i++) {
            Document doc = readerlp.document(i);
            int lpid = Integer.parseInt(doc.get("id"));
            lpid2lp.put(lpid, doc.get("LP"));
            int count = Integer.parseInt(doc.get("count"));
            lpid2count.put(lpid, count);
            lpCount += count;
        }
        readerlp.close();
        Map<Integer, Integer> edp2count = new HashMap<>();
        int entityCount = 0;
        IndexReader readeredp = DirectoryReader.open(FSDirectory.open(Paths.get(PATHS.ProjectData + "EDPIndex/" + dataset + "/")));
        for (int i = 0; i < readeredp.maxDoc(); i++) {
            Document doc = readeredp.document(i);
            int count = Integer.parseInt(doc.get("count"));
            edp2count.put(Integer.parseInt(doc.get("id")), count);
            entityCount += count;
        }
        readeredp.close();
        String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
        Map<String, Set<Integer>> keyword2Component = new HashMap<>();
        IndexSearcher searcher = new IndexSearcher(DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder))));
        for (String iter: keywords) {
            QueryParser parser = new QueryParser("text", new StemAnalyzer());
            Query query = parser.parse(iter);
            TopDocs docs = searcher.search(query, 100000000);
            ScoreDoc[] scores = docs.scoreDocs;
            Set<Integer> compValue = new HashSet<>();
            for (ScoreDoc score: scores) {
                int id = Integer.parseInt(searcher.doc(score.doc).get("id"));
                compValue.add(id);
            }
            keyword2Component.put(iter, compValue);
        }
        double edpNum = entityCount * percent;
        double lpNum = lpCount * percent;
        double wordNum = keywords.size();
        List<List<Integer>> comps  = ReadFile.readInteger(PATHS.ProjectData + "SCPResultWithKeyword/" + pair + "-" + dataset + ".txt", " ");
        double currentEdp = 0;
        Set<Integer> existingEdp = new HashSet<>();
        double currentLp = 0;
        Set<Integer> existingLp = new HashSet<>();
        double currentWord = 0;
        Set<String> existingWord = new HashSet<>();
        for (int comp: comps.get(0)) {
            StringBuilder currentStr = new StringBuilder(dataset + "-" + comp + "\t");
            StringBuilder currentStrWord = new StringBuilder(dataset + "-" + comp + "\t");
            String[] edpStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "edp").split(" ");
            for (String edp: edpStr) {
                int edpId = Integer.parseInt(edp);
                if (currentEdp < edpNum && !existingEdp.contains(edpId)) {
                    currentStr.append(edp).append("\t");
                    currentEdp += edp2count.get(edpId);
                    existingEdp.add(edpId);
                }
            }
            String lpIdStr = IndexUtil.getFieldValue(PATHS.ProjectData + "ComponentIndex/" + dataset + "/", "id", comp, "lp");
            if (lpIdStr.length() > 0) {
                for (String lp: lpIdStr.split(" ")) {
                    int lpId = Integer.parseInt(lp);
                    if (currentLp < lpNum && !existingLp.contains(lpId)) {
                        currentStr.append(lpid2lp.get(lpId)).append("\t");
                        currentLp += lpid2count.get(lpId);
                        existingLp.add(lpId);
                    }
                }
            }
            for (Map.Entry<String, Set<Integer>> iter: keyword2Component.entrySet()) {
                String word = iter.getKey();
                if (currentWord < wordNum && !existingWord.contains(word) && iter.getValue().contains(comp)) {
                    currentStr.append("\"").append(word).append("\"").append("\t");
                    currentStrWord.append(word).append("\t");
                    currentWord += 1;
                    existingWord.add(word);
                }
            }
            String line = currentStr.toString().trim();
            if (line.contains("\t")) {
                allKey.add(line);
                onlyKey.add(currentStrWord.toString().trim());
            }
        }
    }

    // top percent patterns and ALL keywords
    private static void processAllAdd(int start, int end, double percent) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try (PrintWriter writer1 = new PrintWriter(PATHS.FileBase + "file/KeysWithKeywords" + ((int)(percent*100)) + "Add.txt"); //e.g., file/KeysWithKeywords90Add.txt
             PrintWriter writer2 = new PrintWriter(PATHS.FileBase + "file/KeysOnlyKeywords" + ((int)(percent*100)) + "Add.txt")) { // e.g., file/KeysOnlyKeywords90Add.txt
            for (int i = 0; i < pair.size(); i++) {
                List<String> iter = pair.get(i);
                int dataset = Integer.parseInt(iter.get(0));
                if (dataset < start || dataset > end) {
                    continue;
                }
                List<String> keywords = new ArrayList<>(Arrays.asList(iter.get(4).split(" ")));
                List<String> allKey = new ArrayList<>();
                List<String> onlyKey = new ArrayList<>();
                getKeysAllAdd(i, dataset, keywords, percent, allKey, onlyKey);
                if (!allKey.isEmpty()) {
                    for (String s: allKey) {
                        writer1.println(i + "-" + s);
                    }
                    for (String s: onlyKey) {
                        writer2.println(i + "-" + s);
                    }
                    System.out.println("Finish pair " + i + "-" + dataset + ". ");
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getAddCases(String percent) {
        List<String> oldAllKey = ReadFile.readString(PATHS.FileBase + "file/KeysWithKeywords" + percent + ".txt");
        List<String> oldOnlyKey = ReadFile.readString(PATHS.FileBase + "file/KeysOnlyKeywords" + percent + ".txt");
        List<String> newAllKey = ReadFile.readString(PATHS.FileBase + "file/KeysWithKeywords" + percent + "-2.txt");
        List<String> newOnlyKey = ReadFile.readString(PATHS.FileBase + "file/KeysOnlyKeywords" + percent + "-2.txt");
        Map<String, String> oldComp = new HashMap<>();
        for (String iter: oldAllKey) {
            String id = iter.split("\t")[0];
            String value = iter.substring(iter.indexOf("\t") + 1);
            oldComp.put(id, value);
        }
        try {
            PrintWriter writer1 = new PrintWriter(PATHS.FileBase + "file/KeysWithKeywords" + percent + "Add.txt");
            PrintWriter writer2 = new PrintWriter(PATHS.FileBase + "file/KeysOnlyKeywords" + percent + "Add.txt");
            for (int i = 0; i < newOnlyKey.size(); i++) {
                String iter = newAllKey.get(i);
                String id = iter.split("\t")[0];
                String value = iter.substring(iter.indexOf("\t") + 1);
                if (!oldComp.containsKey(id) || !oldComp.get(id).equals(value)) {
                    writer1.println(iter);
                    writer2.println(newOnlyKey.get(i));
                }
            }
            writer1.close();
            writer2.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        processAll(0, 9630, 0.9);
//        processAll(0, 9630, 0.8);

//        processAllAdd(0, 9630, 0.9);
//        processAllAdd(0, 9630, 0.8);

//        getAddCases("80");
//        getAddCases("90");

//    }
}
