package PCSG.abstat;

import DOGST.AnsTree;
import DOGST.CommonStruct;
import DOGST.DOGST;
import DOGST.TreeEdge;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Multigraph;
import PCSG.PATHS;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.Arrays;
import java.util.List;

public class getResultTree {

    public static void getAnswer() {
        List<String> keys = Arrays.asList(ReadFile.readString(PATHS.DBpediaResult + "ComponentIndex80/0.txt").get(0).split(" "));
        CommonStruct c1 = new CommonStruct();
        System.out.println("initiating...");
        c1.Init2(PATHS.DBpediaResult + "KeyKGP/0");
        DOGST k = new DOGST(c1); // keyKG+
        System.out.println("searching...");
        long start = System.currentTimeMillis();
        AnsTree resultTree = k.search(c1, keys, 2); ////////////////////////////////////
        long runtime = System.currentTimeMillis() - start;
        System.out.println("Runtime: " + runtime + " ms. ");
        try (PrintWriter writer = new PrintWriter(PATHS.DBpediaResult + "KeyKGP/0/resultTree.txt")) {
            for (TreeEdge edge: resultTree.edge) {
                writer.print(edge.u + " " + edge.v + ",");
            }
            writer.println();
            writer.println(runtime);
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testConnectivity() {
        Multigraph<Integer, DefaultEdge> graph = new Multigraph<>(DefaultEdge.class);
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "ComponentIndex80/0.txt").get(1).split(",")) {
            int sid = Integer.parseInt(iter.split(" ")[0]);
            int oid = Integer.parseInt(iter.split(" ")[2]);
            graph.addVertex(sid);
            graph.addVertex(oid);
            if (sid != oid) {
                graph.addEdge(sid, oid);
            }
        }
        ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(graph);
        System.out.println(inspector.connectedSets().size());
    }

//    public static void main(String[] args) {
//        getAnswer();
//        testConnectivity();
//    }
}
