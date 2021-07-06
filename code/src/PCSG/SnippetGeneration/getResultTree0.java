package PCSG.SnippetGeneration;

import DOGST.AnsTree;
import DOGST.CommonStruct;
import DOGST.DOGST;
import DOGST.TreeEdge;
import PCSG.PATHS;
import PCSG.beans.KeyKGResultBean;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class getResultTree0 { //WITHOUT KEYWORD!!
    final static int TIMEOUT = 7200000;

    // simple test for one case
    public static void getAnsNoKeywordsTest(int datasetId, int componentId) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/Keys.txt", "\t");
        for (List<String> iter : pair) {
            String id = iter.get(0);
            int dataset = Integer.parseInt(id.split("-")[0]);
            int component = Integer.parseInt(id.split("-")[1]);
            if (dataset != datasetId || component != componentId) {
                continue;
            }
            List<String> keys = iter.subList(1, iter.size());
//            System.out.println("Keys.size() = " + keys.size());
            CommonStruct c1 = new CommonStruct();
            System.out.println("initiating...");
            c1.Init2(PATHS.ProjectData + "KeyKGPNoKeyword/" + id);
            DOGST k = new DOGST(c1); // keyKG+
            System.out.println("searching...");
            long start = System.currentTimeMillis();
            AnsTree resultTree = k.search(c1, keys, 2); ////////////////////////////////////
            long runtime = System.currentTimeMillis() - start;
            System.out.println(id + ": " + runtime + " ms. ");
//            try (PrintWriter writer = new PrintWriter(PATHS.ProjectData + "KeyKGPResult/" + id + ".txt")) {
//                for (TreeEdge edge: resultTree.edge) {
//                    writer.print(edge.u + " " + edge.v + ",");
//                }
//                writer.println();
//                writer.println(runtime);
//            }catch (Exception e) {
//                e.printStackTrace();
//            }
        }
    }

    // test for timeoutService() usage
    public static void processNoKeywordTest(int datasetId, int componentId) {
        List<List<String>> pair = ReadFile.readString(PATHS.FileBase + "file/Keys.txt", "\t");
        for (List<String> iter : pair) {
            String id = iter.get(0);
            int dataset = Integer.parseInt(id.split("-")[0]);
            int component = Integer.parseInt(id.split("-")[1]);
            if (dataset != datasetId || component != componentId) {
                continue;
            }
            List<String> keys = iter.subList(1, iter.size());
            DOGST k = null; // keyKG+
            List<KeyKGResultBean> runningInfo = new ArrayList<>();
            System.out.println(id + " is searching...");
            long runTime = timeoutService(id, k, keys, runningInfo);
            if (runTime == Long.MAX_VALUE) { //timeout
                System.out.println(id + ": TIMEOUT!!");
            }
            else {
                KeyKGResultBean runInfo = runningInfo.get(0);
                System.out.println(id + ": " + runInfo.runtime + " ms. ");
                saveResult(runInfo, PATHS.ProjectData + "KeyKGPResult/");
            }
        }
    }

    // run on all cases, except the singlenodes in file: IsolatedNodes.txt
    public static void processNoKeyword(int start, int end, String caseFile, String resultFolder) {
        List<List<String>> pair = ReadFile.readString(caseFile, "\t");
        Set<String> singleNodes = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodes.txt"));
        for (List<String> iter : pair) {
            String id = iter.get(0);
            int dataset = Integer.parseInt(id.split("-")[0]);
            if (dataset < start || dataset > end || singleNodes.contains(id)) {
                continue;
            }
            List<String> keys = iter.subList(1, iter.size());
//            CommonStruct c1 = new CommonStruct();
//            c1.Init2(PATHS.ProjectData + "KeyKGPNoKeyword/" + id);
//            DOGST k = new DOGST(c1); // keyKG+
            DOGST k = new DOGST();
            List<KeyKGResultBean> runningInfo = new ArrayList<>();
            long runTime = timeoutService(id, k, keys, runningInfo);
            if (runTime == Long.MAX_VALUE) { //timeout
                System.out.println(id + ": TIMEOUT!!");
                if (!k.ansList.isEmpty()) { // try to record the current optimal values when timeout
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
            cs.Init2(PATHS.ProjectData + "KeyKGPNoKeyword/" + id);
            k.InitByCommonStruct(cs);
            resultTree = k.search(cs, keys, 2);
            lastTime = System.currentTimeMillis() - start;
            KeyKGResultBean bean = new KeyKGResultBean(id, resultTree, lastTime);
            runningInfo.add(bean);
        }
    }

//    public static void main(String[] args) {
//        processNoKeyword(Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/Keys80.txt", PATHS.ProjectData + "KeyKGPResult80/");
//    }
}
