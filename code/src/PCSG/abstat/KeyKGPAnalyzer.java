package PCSG.abstat;

import PCSG.PATHS;
import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.*;

public class KeyKGPAnalyzer {

    private static void getResultEntity(String subNameFile, String treeFile, String outputFile) {
        List<Integer> subName = ReadFile.readInteger(subNameFile);
        Set<Integer> entitySet = new TreeSet<>();
        String[] edges = ReadFile.readString(treeFile).get(0).split(",");
        for (String iter: edges) {
            int from = Integer.parseInt(iter.split(" ")[0]);
            int to = Integer.parseInt(iter.split(" ")[1]);
            entitySet.add(subName.get(from));
            entitySet.add(subName.get(to));
        }
        entitySet.add(5101835); //ComponentIndex80/1.txt
        entitySet.add(6530268); //ComponentIndex80/2.txt
        entitySet.add(2872534); //ComponentIndex80/3.txt
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int entity: entitySet) {
                writer.println(entity);
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getEDP2Entity(String entityFile, String outputFile) {
        Map<Integer, Integer> entity2edp = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "entity2edp.txt", "\t")) {
            entity2edp.put(iter.get(0), iter.get(1));
        }
        Map<Integer, Set<Integer>> edp2entity = new TreeMap<>();
        for (int entity: ReadFile.readInteger(entityFile)) {
            int edp = entity2edp.get(entity);
            Set<Integer> entitySet = edp2entity.getOrDefault(edp, new TreeSet<>());
            entitySet.add(entity);
            edp2entity.put(edp, entitySet);
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (Map.Entry<Integer, Set<Integer>> iter: edp2entity.entrySet()) {
                writer.print(iter.getKey() + "\t");
                StringBuilder entityStr = new StringBuilder();
                for (int entity: iter.getValue()) {
                    entityStr.append(entity).append(" ");
                }
                writer.println(entityStr.substring(0, entityStr.length() - 1));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getEDPList(String edp2entityFile, String outputFile) {
        Set<Integer> edpSet = new TreeSet<>();
        for (List<String> iter: ReadFile.readString(edp2entityFile, "\t")) {
            edpSet.add(Integer.parseInt(iter.get(0)));
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int iter: edpSet) {
                writer.println(iter);
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getEntity2Triple(String entityFile, String outputFile) {
        //get entity2class map first
        Map<Integer, Integer> entity2class = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "Entity2Class.txt", "\t")) {
            entity2class.put(iter.get(0), iter.get(1));
        }
        List<List<Integer>> triples = ReadFile.readInteger(PATHS.DBpediaResult + "Triple.txt", "\t");
        Map<Integer, Set<Integer>> entity2coveredProperty = new HashMap<>();
        Map<Integer, Set<Integer>> entity2Triple = new TreeMap<>();
        for (int i = 0; i < triples.size(); i++) {
            List<Integer> iter = triples.get(i);
            int sid = iter.get(0);
            int pid = iter.get(1);
            int oid = iter.get(2);
            Set<Integer> sProp = entity2coveredProperty.getOrDefault(sid, new HashSet<>());
            if (!sProp.contains(pid)) {
                Set<Integer> sTriple = entity2Triple.getOrDefault(sid, new TreeSet<>());
                sTriple.add(i);
                entity2Triple.put(sid, sTriple);
                sProp.add(pid);
                entity2coveredProperty.put(sid, sProp);
            }

            Set<Integer> oProp = entity2coveredProperty.getOrDefault(oid, new HashSet<>());
            if (!oProp.contains(-pid)) {
                Set<Integer> oTriple = entity2Triple.getOrDefault(oid, new TreeSet<>());
                oTriple.add(i);
                entity2Triple.put(oid, oTriple);
                oProp.add(-pid);
                entity2coveredProperty.put(oid, oProp);
            }
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int iter: ReadFile.readInteger(entityFile)) {
                writer.print(iter + "\t" + entity2class.get(iter) + "\t");
                StringBuilder tripleStr = new StringBuilder();
                for (int t: entity2Triple.get(iter)) {
                    List<Integer> triple = triples.get(t);
                    tripleStr.append(triple.get(0)).append(" ").append(triple.get(1)).append(" ").append(triple.get(2)).append(",");
                }
                writer.println(tripleStr.substring(0, tripleStr.length() - 1));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getEDPText(String outputFile) {
        List<String> classList = new ArrayList<>();
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "SnippetResult/Classes.txt")) {
            classList.add(iter.substring(iter.lastIndexOf("/") + 1));
        }
        List<String> labelList = new ArrayList<>();
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "SnippetResult/IRI2ID.txt")) {
            labelList.add(iter.substring(iter.lastIndexOf("/") + 1));
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<String> iter: ReadFile.readString(PATHS.DBpediaResult + "SnippetResult/edpIndex.txt", "\t")) {
                String text = classList.get(Integer.parseInt(iter.get(0))) + "\t";
                if (!iter.get(1).equals("")) {
                    for (String prop: iter.get(1).split(" ")) {
                        text += labelList.get(Integer.parseInt(prop)) + " ";
                    }
                    text = text.substring(0, text.length() - 1);
                }
                text += "\t";
                if (!iter.get(2).equals("")) {
                    for (String prop: iter.get(2).split(" ")) {
                        text += labelList.get(Integer.parseInt(prop)) + " ";
                    }
                    text = text.substring(0, text.length() - 1);
                }
                text += "\t" + iter.get(3);
                writer.println(text);
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        String subNameFile = PATHS.DBpediaResult + "KeyKGP/0/subName.txt";
//        String treeFile = PATHS.DBpediaResult + "treeResult/5.txt";
//        String entityList = PATHS.DBpediaResult + "SnippetResult/entity.txt";
//        getResultEntity(subNameFile, treeFile, entityList);

//        String edp2entityFile = PATHS.DBpediaResult + "SnippetResult/edp2entity.txt";
//        getEDP2Entity(entityList, edp2entityFile);

//        String edpFile = PATHS.DBpediaResult + "SnippetResult/edp.txt";
//        getEDPList(edp2entityFile, edpFile);

//        String entity2tripleFile = PATHS.DBpediaResult + "SnippetResult/entity2triple.txt";
//        getEntity2Triple(entityList, entity2tripleFile);

//        String edpTextFile = PATHS.DBpediaResult + "SnippetResult/edpText.txt";
//        getEDPText(edpTextFile);
//    }
}
