package PCSG.SnippetGeneration;

import PLL.WeightedPLL;
import graph.WeightedGraph;
import PCSG.PATHS;
import PCSG.util.ReadFile;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class getHubLabels {

    private static long generateCHL(String folder) {
        String baseFolder = PATHS.ProjectData + folder;
        long startTime = System.currentTimeMillis();
        try {
            WeightedGraph ww = new WeightedGraph();
            ww.graphIndexRead2(baseFolder);
            WeightedPLL w2 = new WeightedPLL();
            w2.pllIndexDeal2(ww, baseFolder);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - startTime;
    }

    public static void processNoKeyword(int start, int end) {
//        HashSet<String> isolatedNodeSet = new HashSet<>(ReadFile.readString("/home/xxwang/file/IsolatedNodes.txt"));
        HashSet<String> isolatedNodeSet = new HashSet<>(ReadFile.readString(PATHS.ProjectData + "file/IsolatedNodes.txt"));
//        ArrayList<ArrayList<String>> list = ReadFile.readString("/home/xxwang/file/Keys.txt", "\t");
        List<List<String>> list = ReadFile.readString(PATHS.ProjectData + "file/Keys.txt", "\t");
        for (List<String> iter: list) {
            String[] ids = iter.get(0).split("-");
            int dataset = Integer.parseInt(ids[0]);
            if (dataset < start || dataset > end || isolatedNodeSet.contains(iter.get(0))) {
                continue;
            }
            System.out.println("========" + iter.get(0) + "========");
            long runtime = generateCHL("KeyKGPNoKeyword/" + iter.get(0));
        }
    }

    public static void processWithKeyword0(int start, int end) {
//        HashSet<String> isolatedNodeSet = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodesWithKeywords.txt"));
        HashSet<String> isolatedNodeSet = new HashSet<>(ReadFile.readString(PATHS.ProjectData + "file/IsolatedNodesWithKeywords.txt"));
//        ArrayList<ArrayList<String>> list = ReadFile.readString(PATHS.FileBase + "file/KeysOnlyKeywords.txt", "\t");
        List<List<String>> list = ReadFile.readString(PATHS.ProjectData + "file/KeysOnlyKeywords.txt", "\t");
        for (List<String> iter: list) {
            String[] ids = iter.get(0).split("-");
            int dataset = Integer.parseInt(ids[1]);
            if (dataset < start || dataset > end || isolatedNodeSet.contains(iter.get(0))) {
                continue;
            }
            System.out.println("========" + iter.get(0) + "========");
            long runtime = generateCHL("KeyKGPWithKeyword/" + iter.get(0));
        }
    }

    public static void processWithKeyword(int start, int end) {
//        HashSet<String> isolatedNodeSet = new HashSet<>(ReadFile.readString(PATHS.FileBase + "file/IsolatedNodesInCases.txt"));
        Set<String> isolatedNodeSet = new HashSet<>(ReadFile.readString(PATHS.ProjectData + "file/IsolatedNodesInCases.txt"));
//        ArrayList<ArrayList<String>> list = ReadFile.readString(PATHS.FileBase + "file/CasesWithKeywords.txt", "\t");
        List<List<String>> list = ReadFile.readString(PATHS.ProjectData + "file/CasesWithKeywords.txt", "\t");
        for (List<String> iter: list) {
            String[] ids = iter.get(0).split("-");
            int dataset = Integer.parseInt(ids[0]);
            if (dataset < start || dataset > end || isolatedNodeSet.contains(iter.get(0))) {
                continue;
            }
            System.out.println("========" + iter.get(0) + "========");
            long runtime = generateCHL("KeyKGPWithKeyword/" + iter.get(0));
        }
    }

//    public static void main(String[] args) {
//        int start = Integer.parseInt(args[0]);
//        int end = Integer.parseInt(args[1]);
//        processNoKeyword(0, 305); // 306 -- 311: wesley 200G, did not copy to local category due to large size (~300G)
//        processWithKeyword(312, 9630); // 306 -- 311: wesley 200G
//        generateCHL(4, 1);
//    }
}
