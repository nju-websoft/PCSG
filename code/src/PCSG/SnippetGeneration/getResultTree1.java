package PCSG.SnippetGeneration;

import DOGST.AnsTree;
import DOGST.CommonStruct;
import DOGST.DOGST;
import DOGST.TreeEdge;
import PCSG.PATHS;
import PCSG.beans.KeyKGResultBean;
import PCSG.util.ReadFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class getResultTree1 { //WITH KEYWORD!!
    final static int TIMEOUT = 7200000;

    // simple test for one case
    public static void getAnsWithKeywordsTest(int record) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/KeysWithKeywords.txt", "\t");
        List<List<String>> keywords = ReadFile.readString(PATHS.FileBase + "file/KeysOnlyKeywords.txt", "\t");
        List<String> test = pair.get(record - 1); // the record-th case
        String id = test.get(0);
        List<String> keyword = keywords.get(record);
        List<String> keys = test.subList(1, (test.size() - keyword.size() + 1));
        for (int i = 1; i < keyword.size(); i++) {
            keys.add("\"" + keyword.get(i) + "\"");
        }
        CommonStruct c1 = new CommonStruct();
        System.out.println("initiating...");
        c1.Init2(PATHS.ProjectData + "KeyKGPWithKeyword/" + id.substring(id.indexOf("-") + 1));
        DOGST k = new DOGST(c1); // keyKG+
        System.out.println("searching...");
        long start = System.currentTimeMillis();
        AnsTree resultTree = k.search(c1, keys, 2); ////////////////////////////////////
        long runtime = System.currentTimeMillis() - start;
        System.out.println(id + ": " + runtime + " ms. ");
        try (PrintWriter writer = new PrintWriter(PATHS.ProjectData + "KeyKGPWithKeywordResult/" + id + ".txt")) {
            for (TreeEdge edge: resultTree.edge) {
                writer.print(edge.u + " " + edge.v + ",");
            }
            writer.println();
            writer.println(runtime);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // test for timeoutService() usage, record number start from 1
    public static void processWithKeywordTest(int record) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/KeysWithKeywords.txt", "\t");
        List<List<String>> keywords = ReadFile.readString(PATHS.FileBase + "file/KeysOnlyKeywords.txt", "\t");
        List<String> test = pair.get(record - 1); // the record-th case
        String id = test.get(0);
        List<String> keyword = keywords.get(record);
        List<String> keys = test.subList(1, (test.size() - keyword.size() + 1));
        for (int i = 1; i < keyword.size(); i++) {
            keys.add("\"" + keyword.get(i) + "\"");
        }
        DOGST k = null; // keyKG+
        List<KeyKGResultBean> runningInfo = new ArrayList<>();
        System.out.println("searching...");
        long runTime = timeoutService(id, k, keys, runningInfo);
        if (runTime == Long.MAX_VALUE) { //timeout
            System.out.println(id + ": TIMEOUT!!");
        }
        else {
            KeyKGResultBean runInfo = runningInfo.get(0);
            System.out.println(id + ": " + runInfo.runtime + " ms. ");
            saveResult(runInfo, PATHS.ProjectData + "KeyKGPWithKeywordResult/" + runInfo.id + ".txt");
        }
    }

    // run on all cases, except the single nodes in file IsolatedNodesInCases.txt
    public static void processWithKeyword(int start, int end, String allKeyFile, String onlyKeyFile, String baseResultFile, String resultFolder) {
        List<List<String>> pair = ReadFile.readString(allKeyFile, "\t");
        List<List<String>> keywords = ReadFile.readString(onlyKeyFile, "\t");
        Set<String> noWords = new HashSet<>(); // to handle the components without keywords
        try (BufferedReader reader = new BufferedReader(new FileReader(baseResultFile))) {
            String line = "";
            while ((line = reader.readLine()) != null) {
                noWords.add(line.split("\t")[0]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodesInCases.txt"));
//        Set<String> timeoutCases = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/TimeoutNoKeyword.txt"));
        for (int i = 0; i < pair.size(); i++) {
            List<String> iter = pair.get(i);
            List<String> keyword = keywords.get(i);
            String id = iter.get(0);
            String component = id.substring(id.indexOf("-") + 1);
            int dataset = Integer.parseInt(id.split("-")[1]);
            if (dataset < start || dataset > end || singleNodes.contains(component) ) {
                continue;
            }
            File file = new File(resultFolder + component + ".txt");
            if (keyword.size() == 1 && (noWords.contains(component) || file.exists()) ) {
                continue;
            }
            List<String> keys = iter.subList(1, (iter.size() - keyword.size() + 1));
            for (int j = 1; j < keyword.size(); j++) {
                keys.add("\"" + keyword.get(j) + "\"");
            }
//            CommonStruct c1 = new CommonStruct();
//            c1.Init2(PATHS.ProjectData + "KeyKGPWithKeyword/" + id.substring(id.indexOf("-") + 1));
//            DOGST k = new DOGST(c1); // keyKG+
            DOGST k = new DOGST();
            List<KeyKGResultBean> runningInfo = new ArrayList<>();
            long runTime = timeoutService(id, k, keys, runningInfo);
            if (runTime == Long.MAX_VALUE) { //timeout
                System.out.println(id + ": TIMEOUT!!");
                if (k.ansList != null) { // try to record the current optimal values when timeout
                    double totalweight = Double.MAX_VALUE;
                    AnsTree bestTree = null;
                    for (AnsTree anst : k.ansList) {
                        if (totalweight > anst.weight) {
                            totalweight = anst.weight;
                            bestTree = anst;
                        }
                    }
                    KeyKGResultBean runInfo = new KeyKGResultBean(id, bestTree, TIMEOUT);
                    saveResult(runInfo, resultFolder);
                }
            }
            else {
                KeyKGResultBean runInfo = runningInfo.get(0);
                System.out.println(id + ": " + runInfo.runtime + " ms. ");
                saveResult(runInfo, resultFolder);
            }
        }
    }

    // note the folder where the AnsTree were stored
    public static void saveResult(KeyKGResultBean runInfo, String resultFolder) {
        try (PrintWriter writer = new PrintWriter(resultFolder + runInfo.id + ".txt")) {
            for (TreeEdge edge: runInfo.resultTree.edge) {
                writer.print(edge.u + " " + edge.v + ",");
            }
            writer.println();
            writer.println(runInfo.runtime);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long timeoutService(String id, DOGST k, List<String> keys, List<KeyKGResultBean> runningInfo) {
        long time = Long.MAX_VALUE;
        CustomedThread subThread = new CustomedThread(id, k, keys, runningInfo);
        subThread.start();
        try {
            subThread.join(TIMEOUT); //waiting for subThread to finish

            if(!subThread.isAlive()) {
                time = subThread.lastTime; // already finished
            }
//            subThread.interrupt();
            subThread.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return time;
    }

    private static class CustomedThread extends Thread{
        String id;
        DOGST k;
        List<String> keys;
        AnsTree resultTree;
        long lastTime = Long.MAX_VALUE;
        List<KeyKGResultBean> runningInfo;

        public CustomedThread(String id, DOGST k, List<String> keys, List<KeyKGResultBean> runningInfo) {
            super();
            this.id = id;
            this.k = k;
            this.keys = keys;
            this.runningInfo = runningInfo;
        }
        @Override
        public void run() {
            long start = System.currentTimeMillis();
            CommonStruct cs = new CommonStruct();
            if (id.split("-").length == 2) {
                cs.Init2(PATHS.ProjectData + "KeyKGPWithKeyword/" + id);
            }
            else {
                cs.Init2(PATHS.ProjectData + "KeyKGPWithKeyword/" + id.substring(id.indexOf("-") + 1));
            }
            k.InitByCommonStruct(cs);
            resultTree = k.search(cs, keys, 2);
            lastTime = System.currentTimeMillis() - start;
            KeyKGResultBean bean = new KeyKGResultBean(id, resultTree, lastTime);
            runningInfo.add(bean);
        }
    }

//    public static void main(String[] args) {
//        processWithKeyword(Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/KeysWithKeywords.txt", PATHS.FileBase + "file/KeysOnlyKeywords.txt", PATHS.FileBase + "file/Keys.txt", PATHS.ProjectData + "KeyKGPWithKeywordResult/");

//        String allKeyFile = PATHS.FileBase + "file/KeysWithKeywords90Add.txt";
//        String onlyKeyFile = PATHS.FileBase + "file/KeysOnlyKeywords90Add.txt";
//        String baseResultFile = PATHS.FileBase + "file/Keys90.txt";
//        String resultFolder = PATHS.ProjectData + "KeyKGPWithKeywordResult90Add/";
//        processWithKeyword(Integer.parseInt(args[0]), Integer.parseInt(args[1]), allKeyFile, onlyKeyFile, baseResultFile, resultFolder);
//    }
}
