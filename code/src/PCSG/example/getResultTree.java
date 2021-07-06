package PCSG.example;

import DOGST.AnsTree;
import DOGST.CommonStruct;
import DOGST.DOGST;
import DOGST.TreeEdge;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.*;

public class getResultTree {

    private static void storeKeys(String folder) {
        Map<Integer, String> lpId2LP = new HashMap<>();
        for (List<String> iter: ReadFile.readString(folder + "LP.txt", "\t")) {
            lpId2LP.put(Integer.parseInt(iter.get(0)), iter.get(1));
        }
        try {
            PrintWriter writer = new PrintWriter(folder + "keys.txt");
            for (int comp: ReadFile.readInteger(folder + "set-cover-components.txt")){
                String keyStr = "";
                for (String iter: ReadFile.readString(folder + "component/" + comp + "/edp.txt")) {
                    keyStr += iter + "\t";
                }
                for (Integer iter: ReadFile.readInteger(folder + "component/" + comp + "/lp.txt")) {
                    keyStr += lpId2LP.get(iter) + "\t";
                }
                writer.println(comp + "\t" + keyStr.trim());
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getAnswerTree(String folder, List<String> keys) {
        CommonStruct c1 = new CommonStruct();
        c1.Init2(folder);
        DOGST k = new DOGST(c1); // keyKG+
        long start = System.currentTimeMillis();
        AnsTree resultTree = k.search(c1, keys, 2);
        long runtime = System.currentTimeMillis() - start;
        try{
            PrintWriter writer = new PrintWriter(folder + "/result-tree.txt");
            for (TreeEdge edge: resultTree.edge) {
//                System.out.println(edge.u + " " + edge.v + ",");
                writer.print(edge.u + " " + edge.v + ",");
            }
//            writer.println();
//            writer.println(runtime);
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getTreeforAllComponents(String folder) {
        for (List<String> iter: ReadFile.readString(folder + "keys.txt", "\t")) {
            String compFolder = folder + "component/" + iter.get(0);
            List<String> keys = iter.subList(1, iter.size());
//            System.out.println("Result of component " + iter.get(0) + ": ");
            getAnswerTree(compFolder, keys);
        }
    }

    private static Set<List<Integer>> tree2triples(int typeID, Set<Integer> literalSet, String folder) {
        Map<Integer, Set<List<Integer>>> entity2triple = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(folder + "/graph.txt", " ")) {
            int subject = iter.get(0);
            int predicate = iter.get(1);
            int object = iter.get(2);
            if (predicate == typeID || literalSet.contains(object)) { // do not need to be added into the graph
                Set<List<Integer>> subjectTripleSet = entity2triple.getOrDefault(subject, new HashSet<>());
                subjectTripleSet.add(iter);
                entity2triple.put(subject, subjectTripleSet);
            }
            else {
                Set<List<Integer>> subjectTripleSet = entity2triple.getOrDefault(subject, new HashSet<>());
                subjectTripleSet.add(iter);
                entity2triple.put(subject, subjectTripleSet);
                Set<List<Integer>> objectTripleSet = entity2triple.getOrDefault(object, new HashSet<>());
                objectTripleSet.add(iter);
                entity2triple.put(object, objectTripleSet);
            }
        }
        Set<List<Integer>> result = new HashSet<>();
        Map<Integer, Set<Integer>> entity2prop = new HashMap<>();
        Set<Integer> entitySet = new HashSet<>();
        List<String> subName = ReadFile.readString(folder + "/subName.txt");
        for (String edge: ReadFile.readString(folder + "/result-tree.txt").get(0).split(",")) {
            String from = subName.get(Integer.parseInt(edge.split(" ")[0]));
            String to = subName.get(Integer.parseInt(edge.split(" ")[1]));
            String triple;
            if (from.split(" ").length == 3) {
                triple = from;
            }
            else {
                triple = to;
            }
            int subject = Integer.parseInt(triple.split(" ")[0]);
            int predicate = Integer.parseInt(triple.split(" ")[1]);
            int object = Integer.parseInt(triple.split(" ")[2]);
            result.add(new ArrayList<>(Arrays.asList(subject, predicate, object)));
            entitySet.add(subject);
            entitySet.add(object);
            Set<Integer> subjectProp = entity2prop.getOrDefault(subject, new HashSet<>());
            subjectProp.add(predicate);
            entity2prop.put(subject, subjectProp);
            Set<Integer> objectProp = entity2prop.getOrDefault(object, new HashSet<>());
            objectProp.add(-predicate);
            entity2prop.put(object, objectProp);
        }
        System.out.println(result);/////////////////////////////
        for (int entity: entitySet) {
            Set<Integer> cpSet = entity2prop.get(entity);
            for (List<Integer> triple: entity2triple.get(entity)) {
                if (entity == triple.get(0)) {
                    if (triple.get(1) == typeID && !cpSet.contains(triple.get(2))) {
                        result.add(triple);
                        cpSet.add(triple.get(2));
                    }
                    else if (triple.get(1) != typeID && !cpSet.contains(triple.get(1))) {
                        result.add(triple);
                        cpSet.add(triple.get(1));
                    }
                }
                else {
                    if (!cpSet.contains(-triple.get(1))) {
                        result.add(triple);
                        cpSet.add(-triple.get(1));
                    }
                }
            }
        }
        return result;
    }

    private static void getSnippetFromTree(String folder) {
        Set<List<Integer>> result = new HashSet<>();
        int typeID = DatasetIndexer.getTypeID(folder + "label.txt");
        Set<Integer> literalSet = DatasetIndexer.getLiteralSet(folder + "label.txt");
        for (int comp: ReadFile.readInteger(folder + "set-cover-components.txt")) {
            String compFolder = folder + "component/" + comp;
            result.addAll(tree2triples(typeID, literalSet, compFolder));
        }
//        for (List<Integer> iter: result) {
//            System.out.println(iter.get(0) + "\t" + iter.get(1) + "\t" + iter.get(2));
//        }
    }

    public static void main(String[] args) {
        String folder = "C:/Users/Desktop/example/"; // change to your local folder where there are graph.txt and label.txt files.
        storeKeys(folder);
        getTreeforAllComponents(folder);
        getSnippetFromTree(folder);
    }
}
