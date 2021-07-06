package PCSG.KSD;

import PCSG.PATHS;
import PCSG.beans.KSDTriple;
import PCSG.beans.ResultBean;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.*;

public class generateTest {

    private final static int TIMEOUT = 7200000;

    private static void getResult(int start, int end, String caseFile, String resultFolder){
        List<List<String>> pairs = ReadFile.readString(caseFile, "\t");
        try {
            for (List<String> pair: pairs){
                int id = Integer.parseInt(pair.get(0));
                if (id < start || id > end) {
                    continue;
                }
                int dataset = Integer.parseInt(pair.get(1));
                int MAX_SIZE = Integer.parseInt(pair.get(2));
                ArrayList<ResultBean> runningInfos = new ArrayList<>();
                List<String> keywords = Arrays.asList(pair.get(3).split(" "));
                System.out.println(id + "----" + dataset + ": " + keywords);
                KSDSnippet finder = new KSDSnippet(dataset, MAX_SIZE);
                long runTime = timoutService(id, finder, dataset, keywords, runningInfos, TIMEOUT);//finder
                // Here, ``id'' is used to identify the row of all cases, not the specific query-dataset-pair
                boolean timeout = runTime == Long.MAX_VALUE;
                if (timeout) {
                    System.out.println("Time out");
                    StringBuilder keyword = new StringBuilder();
                    for (String s : keywords) {
                        keyword.append(s).append(" ");
                    }
                    Set<Integer> ids = new HashSet<>();
                    StringBuilder triplestr = new StringBuilder();
                    Set<KSDTriple> result = finder.result;
                    for (KSDTriple iter: result){
                        int sid = iter.getSid();
                        int oid = iter.getOid();
                        int pid = iter.getPid();
                        ids.add(sid);
                        ids.add(oid);
                        triplestr.append(sid).append(" ").append(oid).append(" ").append(pid).append(",");
                    }
                    StringBuilder idstr = new StringBuilder();
                    for (int iter: ids){
                        idstr.append(iter).append(",");
                    }
                    String snippetstr = "";
                    if (!idstr.toString().equals("")) {
                        snippetstr = idstr.substring(0, idstr.length() - 1) + ";" + triplestr.substring(0, triplestr.length() - 1);
                    }
                    ResultBean bean = new ResultBean(id, dataset, keyword.toString().trim(), snippetstr, TIMEOUT);
                    saveRecord(bean, resultFolder);
                    continue;
                }

                System.out.println("Finish in: " + runTime + " ms. ");
                ResultBean middleRuntimeBean = runningInfos.get(0);
                saveRecord(middleRuntimeBean, resultFolder);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void getResultBase(int start, int end, String resultFolder){
        List<List<String>> pairs = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t");
        try {
            for (int i = 0; i < pairs.size(); i++){
                if (i < start || i > end) {
                    continue;
                }
                List<String> pair = pairs.get(i);
                int dataset = Integer.parseInt(pair.get(0));
                int MAX_SIZE = 20;
                ArrayList<ResultBean> runningInfos = new ArrayList<>();
                List<String> keywords = Arrays.asList(pair.get(4).split(" "));
                System.out.println(i + "----" + dataset + ": " + keywords);
                KSDSnippet finder = new KSDSnippet(dataset, MAX_SIZE);
                long runTime = timoutService(i, finder, dataset, keywords, runningInfos, TIMEOUT);//finder
                // Here, ``id'' is used to identify the row of all cases, not the specific query-dataset-pair
                boolean timeout = runTime == Long.MAX_VALUE;
                if (timeout) {
                    System.out.println("Time out");
                    StringBuilder keyword = new StringBuilder();
                    for (String s : keywords) {
                        keyword.append(s).append(" ");
                    }
                    Set<Integer> ids = new HashSet<>();
                    StringBuilder triplestr = new StringBuilder();
                    Set<KSDTriple> result = finder.result;
                    for (KSDTriple iter: result){
                        int sid = iter.getSid();
                        int oid = iter.getOid();
                        int pid = iter.getPid();
                        ids.add(sid);
                        ids.add(oid);
                        triplestr.append(sid).append(" ").append(oid).append(" ").append(pid).append(",");
                    }
                    StringBuilder idstr = new StringBuilder();
                    for (int iter: ids){
                        idstr.append(iter).append(",");
                    }
                    String snippetstr = "";
                    if (!idstr.toString().equals("")) {
                        snippetstr = idstr.substring(0, idstr.length() - 1) + ";" + triplestr.substring(0, triplestr.length() - 1);
                    }
                    ResultBean bean = new ResultBean(i, dataset, keyword.toString().trim(), snippetstr, TIMEOUT);
                    saveRecord(bean, resultFolder);
                    continue;
                }

                System.out.println("Finish in: " + runTime + " ms. ");
                ResultBean middleRuntimeBean = runningInfos.get(0);
                saveRecord(middleRuntimeBean, resultFolder);
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private static void saveRecord(ResultBean bean, String resultFolder) {
        try {
            PrintWriter writer = new PrintWriter(resultFolder + bean.id + "-" + bean.dataset + ".txt");
            writer.println(bean.snippet);
            writer.println(bean.keyword);
            writer.println(bean.runningTime);
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long timoutService(int id, KSDSnippet finder, int datasetId, List<String> keywords, List<ResultBean> runningInfos, long timeout){
        long time = Long.MAX_VALUE;
        CustomedThread subThread = new CustomedThread(id, finder, datasetId, keywords, runningInfos);
        subThread.start();
        try {
            subThread.join(timeout);

            if(!subThread.isAlive())
                time = subThread.lastTime;

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        subThread.stop();
        return time;
    }

    private static class CustomedThread extends Thread{
        public int id;
        public long lastTime = Long.MAX_VALUE;
        KSDSnippet finder;
        int datasetId;
        List<String> keywords;
        Set<KSDTriple> result;
        List<ResultBean> runningInfos;
        public CustomedThread(int id, KSDSnippet finder, int datasetId, List<String> keywords, List<ResultBean> runningInfos) {
            super();
            this.id = id;
            this.finder = finder;
            this.datasetId = datasetId;
            this.keywords = keywords;
            this.runningInfos=runningInfos;
        }
        @Override
        public void run(){
            long start = System.currentTimeMillis();
            try {
                finder.findSnippet(keywords);
                result = finder.result;
            } catch (Exception e) {
                e.printStackTrace();
            }
            lastTime = System.currentTimeMillis()-start;
            Set<Integer> ids = new HashSet<>();
            StringBuilder triplestr = new StringBuilder();
            for (KSDTriple iter: result){
                int sid = iter.getSid();
                int oid = iter.getOid();
                int pid = iter.getPid();
                ids.add(sid);
                ids.add(oid);
                triplestr.append(sid).append(" ").append(oid).append(" ").append(pid).append(",");
            }
            StringBuilder idstr = new StringBuilder();
            for (int iter: ids){
                idstr.append(iter).append(",");
            }
            String snippetstr = idstr.substring(0, idstr.length() - 1) + ";" + triplestr.substring(0, triplestr.length() - 1);
            StringBuilder keyword = new StringBuilder();
            for (String s : keywords) {
                keyword.append(s).append(" ");
            }
            ResultBean bean = new ResultBean(id, datasetId, keyword.toString().trim(), snippetstr, lastTime);
            runningInfos.add(bean);
        }
    }

//    public static void main(String[] args){
//        getResult( Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/KSDcases.txt", PATHS.ProjectData + "KSDResult/");
//        getResult( Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/KSDcases90.txt", PATHS.ProjectData + "KSDResult90/");
//        getResult( Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/KSDcases80Add.txt", PATHS.ProjectData + "KSDResult80Add/");
//    }

}
