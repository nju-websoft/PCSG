package PCSG.illusnip;

import PCSG.PATHS;
import PCSG.beans.OPTTriple;
import PCSG.beans.ResultBean;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.*;

public class generateTest {
    private final static int TIMEOUT = 7200000;

    private void getResult(int start, int end, String sizeFile, String resultFolder){
        List<List<Integer>> dataset2Size = ReadFile.readInteger(sizeFile, "\t");
        for (List<Integer> ds: dataset2Size){
            int dataset = ds.get(0);
            if (dataset < start || dataset > end) {
                continue;
            }
            int MAX_SIZE = ds.get(1);
            System.out.println("========" + dataset + ": " + MAX_SIZE  + "========");
            OPTRank finder = new OPTRank(dataset, MAX_SIZE); // ======== MAX_SIZE ========
            List<ResultBean> runningInfos = new ArrayList<>();
            long runTime = timoutService(finder, dataset, runningInfos, TIMEOUT);//finder
            boolean timeout = (runTime == Long.MAX_VALUE);
            if (timeout) {
                System.out.println("Time out");
                Set<Integer> ids = new HashSet<>();
                StringBuilder triplestr = new StringBuilder();
                Set<OPTTriple> result = finder.result;
                if (result.isEmpty()) {
                    result = finder.currentSnippet;
                }
                for (OPTTriple iter: result){
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
                ResultBean bean = new ResultBean(dataset, snippetstr, TIMEOUT);
                saveResult(bean, resultFolder);
                continue;
            }
            System.out.println("Finish in: " + runTime + " ms. ");
            ResultBean middleRuntimeBean = runningInfos.get(0);
            saveResult(middleRuntimeBean, resultFolder);
        }
    }

    private void getResultBase(int start, int end, String resultFolder){
        List<Integer> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0);
        for (int dataset: datasets){
            if (dataset < start || dataset > end) {
                continue;
            }
            int MAX_SIZE = 20;
            System.out.println("========" + dataset + ": " + MAX_SIZE  + "========");
            OPTRank finder = new OPTRank(dataset, MAX_SIZE); // ======== MAX_SIZE ========
            List<ResultBean> runningInfos = new ArrayList<>();
            long runTime = timoutService(finder, dataset, runningInfos, TIMEOUT);//finder
            boolean timeout = (runTime == Long.MAX_VALUE);
            if (timeout) {
                System.out.println("Time out");
                Set<Integer> ids = new HashSet<>();
                StringBuilder triplestr = new StringBuilder();
                Set<OPTTriple> result = finder.result;
                if (result.isEmpty()) {
                    result = finder.currentSnippet;
                }
                for (OPTTriple iter: result){
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
                ResultBean bean = new ResultBean(dataset, snippetstr, TIMEOUT);
                saveResult(bean, resultFolder);
                continue;
            }
            System.out.println("Finish in: " + runTime + " ms. ");
            ResultBean middleRuntimeBean = runningInfos.get(0);
            saveResult(middleRuntimeBean, resultFolder);
        }
    }

    private void saveResult(ResultBean bean, String resultFolder) {
        try {
            PrintWriter writer = new PrintWriter(resultFolder + bean.dataset + ".txt");
            writer.println(bean.snippet);
            writer.println(bean.runningTime);
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static long  timoutService(OPTRank finder, int datasetId, List<ResultBean> runningInfos, long timeout){
        long time = Long.MAX_VALUE;
        CustomedThread subThread = new CustomedThread(finder, datasetId, runningInfos);
        subThread.start();
        try {
            subThread.join(timeout);

            if(!subThread.isAlive())
                time = subThread.lastTime;

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        subThread.interrupt();
        return time;
    }

    private static class CustomedThread extends Thread{
        public long lastTime = Long.MAX_VALUE;
        OPTRank finder;
        int datasetId;
        Set<OPTTriple> result;
        List<ResultBean> runningInfos;
        public CustomedThread(OPTRank finder, int datasetId, List<ResultBean> runningInfos) {
            super();
            this.finder = finder;
            this.datasetId = datasetId;
            this.runningInfos = runningInfos;
        }
        @Override
        public void run(){
            long start = System.currentTimeMillis();
            try {
                finder.findSnippet();
                result = finder.result;
            } catch (Exception e) {
                e.printStackTrace();
            }
            lastTime = System.currentTimeMillis() - start;
            /**开始建snippetString*/
            Set<Integer> ids = new HashSet<>();
            StringBuilder triplestr = new StringBuilder();
            for (OPTTriple iter: result){
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
            ResultBean bean = new ResultBean(datasetId, snippetstr, lastTime);
            runningInfos.add(bean);
        }
    }

//    public static void main(String[] args){
//        generateTest test = new generateTest();
//        test.getResult(Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/SnippetResultCount90.txt", PATHS.ProjectData + "IlluSnipResult90/");
//        test.getResult(Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.FileBase + "file/SnippetResultCount80.txt", PATHS.ProjectData + "IlluSnipResult80/");
//        test.getResultBase(Integer.parseInt(args[0]), Integer.parseInt(args[1]), PATHS.ProjectData + "IlluSnipResult20/");
//    }
}
