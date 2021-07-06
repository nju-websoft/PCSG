package PCSG.abstat;

import PLL.WeightedPLL;
import graph.WeightedGraph;
import org.jgrapht.alg.connectivity.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Multigraph;
import PCSG.PATHS;
import PCSG.util.ReadFile;

import java.io.File;
import java.io.PrintWriter;
import java.util.*;

public class DatasetIndexer {

    private static void getIRIMap(String relationFile, String outputIRIFile, String outputTripleFile) {
        Set<String> iriSet = new TreeSet<>();
        for (String line: ReadFile.readString(relationFile)) {
            String spo[] = line.split("\\s+");
            iriSet.add(spo[0].substring(1, spo[0].length() - 1));
            iriSet.add(spo[1].substring(1, spo[1].length() - 1));
            iriSet.add(spo[2].substring(1, spo[2].length() - 1));
        }
        try {
            Map<String, Integer> iri2id = new HashMap<>();
            PrintWriter iriWriter = new PrintWriter(outputIRIFile);
            int count = 0;
            for (String iter: iriSet) {
                iri2id.put(iter, count);
                count++;
                iriWriter.println(iter);
            }
            iriWriter.close();
            PrintWriter tripleWriter = new PrintWriter(outputTripleFile);
            for (String line: ReadFile.readString(relationFile)) {
                String spo[] = line.split("\\s+");
                int sid = iri2id.get(spo[0].substring(1, spo[0].length() - 1));
                int pid = iri2id.get(spo[1].substring(1, spo[1].length() - 1));
                int oid = iri2id.get(spo[2].substring(1, spo[2].length() - 1));
                tripleWriter.println(sid + "\t" + pid + "\t" + oid);
            }
            tripleWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getEntityClass(String typeFile, String relationFile, String iriFile, String outputClassFile, String outputMapFile) {
        String defaultClass = "http://www.w3.org/2002/07/owl#Thing";
        Map<String, String> existingType = new HashMap<>();
        for (String line: ReadFile.readString(typeFile)) {
            if (!line.startsWith("<")) {
                continue;
            }
            String[] spo = line.split("\\s+");
            existingType.put(spo[0].substring(1, spo[0].length() - 1), spo[2].substring(1, spo[2].length() - 1));
        }
        Map<String, Integer> iri2id = new HashMap<>();
        int i = 0;
        for (String iri: ReadFile.readString(iriFile)) {
            iri2id.put(iri, i);
            i++;
        }
        Set<Integer> allEntity = new TreeSet<>(); // all ids of IRI appear as subject or object, subset of the keyset of iri2id
        Set<String> allClass = new TreeSet<>(); // all classes used in the relation file
        Map<Integer, String> entity2class = new HashMap<>();
        for (String iter: ReadFile.readString(relationFile)) {
            String spo[] = iter.split("\\s+");
            String subject = spo[0].substring(1, spo[0].length() - 1);
            String object = spo[2].substring(1, spo[2].length() - 1);

            String subjectClass = existingType.getOrDefault(subject, defaultClass);
            int sid = iri2id.get(subject);
            entity2class.put(sid, subjectClass);
            allEntity.add(sid);
            allClass.add(subjectClass);

            String objectClass = existingType.getOrDefault(object, defaultClass);
            int oid = iri2id.get(object);
            entity2class.put(oid, objectClass);
            allEntity.add(oid);
            allClass.add(objectClass);
        }
        System.out.println("AllClass size: " + allClass.size());
        Map<String, Integer> class2id = new HashMap<>(); // iri -> integer
        try {
            PrintWriter classWriter = new PrintWriter(outputClassFile);
            int count = 0;
            for (String citer: allClass) {
                class2id.put(citer, count);
                count++;
                classWriter.println(citer);
            }
            classWriter.close();
            PrintWriter entity2classWriter = new PrintWriter(outputMapFile);
            for (int entity: allEntity) {
                entity2classWriter.println(entity + "\t" + class2id.get(entity2class.get(entity)));
            }
            entity2classWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // count the number of distinct connected components
    private static void countComponents(String tripleFile) {
        Multigraph<Integer, DefaultEdge> graph = new Multigraph<>(DefaultEdge.class);
//        List<String> iriList = ReadFile.readString(PATHS.DBpediaResult + "IRI2ID.txt");
        for (List<Integer> iter: ReadFile.readInteger(tripleFile, "\t")) {
            int sid = iter.get(0);
//            int pid = iter.get(1);
            int oid = iter.get(2);
            graph.addVertex(sid);
            graph.addVertex(oid);
            if (sid != oid) {
                graph.addEdge(sid, oid);
            }
//            else {
//                System.out.println(iriList.get(sid) + "\t" + iriList.get(pid) + "\t" + iriList.get(oid));
//            }
        }
        ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(graph);
        System.out.println("#Components: " + inspector.connectedSets().size());
    }

    private static Map<Integer, Integer> ReadClassMap() {
        Map<Integer, Integer> result = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "Entity2Class.txt", "\t")) {
            result.put(iter.get(0), iter.get(1));
        }
        return result;
    }

    private static Map<String, Integer> ReadIRIMap() {
        Map<String, Integer> result = new HashMap<>();
        int count = 0;
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "IRI2ID.txt")) {
            result.put(iter, count);
            count++;
        }
        return result;
    }

    private static Map<Integer, Integer> ReadEDP2CountMap() {
        Map<Integer, Integer> result = new HashMap<>();
        int count = 0;
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "edpIndex.txt")) {
            result.put(count, Integer.parseInt(iter.split("\t")[3]));
            count++;
        }
        return result;
    }

    private static void getPatternIndex(String outputFolder) {
        Map<Integer, List<Set<Integer>>> entity2pattern = new HashMap<>();
        for (Map.Entry<Integer, Integer> iter: ReadClassMap().entrySet()) { // value.get(0) - class; value.get(1) - out property; value.get(2) - in property
            List<Set<Integer>> value = new ArrayList<>();
            value.add(new HashSet<>(Arrays.asList(iter.getValue())));
            value.add(new HashSet<>());
            value.add(new HashSet<>());
            entity2pattern.put(iter.getKey(), value);
        }
        for (List<Integer> triple: ReadFile.readInteger(PATHS.DBpediaResult + "Triple.txt", "\t")) {
            int sid = triple.get(0);
            int pid = triple.get(1);
            int oid = triple.get(2);
            entity2pattern.get(sid).get(1).add(pid);
            entity2pattern.get(oid).get(2).add(pid);
        }
        Map<List<Set<Integer>>, Integer> edp2count = new HashMap<>();
        for (Map.Entry<Integer, List<Set<Integer>>> iter: entity2pattern.entrySet()) {
            int count = edp2count.getOrDefault(iter.getValue(), 0);
            edp2count.put(iter.getValue(), count + 1);
        }
        List<Map.Entry<List<Set<Integer>>, Integer>> toBeSortList = new ArrayList<>(edp2count.entrySet());
        Collections.sort(toBeSortList, new Comparator<Map.Entry<List<Set<Integer>>, Integer>>() {
            @Override
            public int compare(Map.Entry<List<Set<Integer>>, Integer> o1, Map.Entry<List<Set<Integer>>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        try {
            Map<List<Set<Integer>>, Integer> edp2id = new HashMap<>();
            int count = 0;
            PrintWriter edpWriter = new PrintWriter(outputFolder + "edpIndex.txt");
            for (Map.Entry<List<Set<Integer>>, Integer> iter: toBeSortList) {
                edp2id.put(iter.getKey(), count);
                count++;
                StringBuilder edp = new StringBuilder();
                for (int id: iter.getKey().get(0)) {
                    edp.append(id);
                }
                edp.append("\t");
                if (!iter.getKey().get(1).isEmpty()) {
                    for (int id: iter.getKey().get(1)) {
                        edp.append(id).append(" ");
                    }
                    edp = new StringBuilder(edp.substring(0, edp.length() - 1));
                }
                edp.append("\t");
                if (!iter.getKey().get(2).isEmpty()) {
                    for (int id: iter.getKey().get(2)) {
                        edp.append(id).append(" ");
                    }
                    edp = new StringBuilder(edp.substring(0, edp.length() - 1));
                }
                edpWriter.println(edp + "\t" + iter.getValue());
            }
            edpWriter.close();

            PrintWriter entity2edpWriter = new PrintWriter(outputFolder + "entity2edp.txt");
            for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "entity2class.txt", "\t")) {
                int entity = iter.get(0);
                entity2edpWriter.println(entity + "\t" + edp2id.get(entity2pattern.get(entity)));
            }
            entity2edpWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static int getEDPpercent(double percent) {
        int entitySize = ReadFile.readString(PATHS.DBpediaResult + "Entity2Class.txt").size();
        int count = 0;
        int line = 0;
        for (String iter: ReadFile.readString(PATHS.DBpediaResult + "edpIndex.txt")) {
            count += Integer.parseInt(iter.split("\t")[3]);
            line++;
            if (count >= percent * entitySize) {
                System.out.println(percent + " - " + "line: " + line + ", count: " + Integer.parseInt(iter.split("\t")[3]));
                break;
            }
        }
        return line;
    }

    private static void testEDPNumberinComponent() { //80%: 2403, 75%: 1085, 70%: 524
        Map<Integer, Integer> entity2edp = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "entity2edp.txt", "\t")) {
            if (iter.get(1) >= 1085) continue;
            entity2edp.put(iter.get(0), iter.get(1));
        }
        Multigraph<Integer, DefaultEdge> graph = new Multigraph<>(DefaultEdge.class);
        int count = 1;
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "Triple.txt", "\t")) {
            int sid = iter.get(0);
            int oid = iter.get(2);
            int link = -(count); // 从1开始！
            count++;
            graph.addVertex(sid);
            graph.addVertex(oid);
            graph.addVertex(link);
            graph.addEdge(sid, link);
            graph.addEdge(link, oid);
        }
        ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(graph);
        List<Set<Integer>> components = inspector.connectedSets();
        Map<Set<Integer>, Integer> component2Size = new HashMap<>();
        for (Set<Integer> comp: components) {
            Set<Integer> edpSet = new TreeSet<>(); //instantiated edps in the component
            for (int node: comp) {
                int edp = entity2edp.getOrDefault(node, -1);
                if (edp != -1) {
                    edpSet.add(edp);
                }
            }
            component2Size.put(comp, edpSet.size());
        }
        List<Map.Entry<Set<Integer>, Integer>> toBeSortList = new ArrayList<>(component2Size.entrySet());
        Collections.sort(toBeSortList, new Comparator<Map.Entry<Set<Integer>, Integer>>() {
            @Override
            public int compare(Map.Entry<Set<Integer>, Integer> o1, Map.Entry<Set<Integer>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        System.out.println("ALL components: " + toBeSortList.size());
        int i = 0;
        for (Map.Entry<Set<Integer>, Integer> iter: toBeSortList) {
            if (i > 1000) {
                break;
            }
            System.out.println(i + "\t" + iter.getValue());
            i++;
        }
    }

    private static void getComponentIndex(String outputFolder, int edpNum) {
        Map<Integer, Integer> entity2edp = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "entity2edp.txt", "\t")) {
            if (iter.get(1) >= edpNum) continue;
            entity2edp.put(iter.get(0), iter.get(1));
        }
        Multigraph<Integer, DefaultEdge> graph = new Multigraph<>(DefaultEdge.class);
        Map<Integer, Set<List<Integer>>> entity2Triple = new HashMap<>();
        int count = 1;
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "Triple.txt", "\t")) {
            int sid = iter.get(0);
            int oid = iter.get(2);
            int link = -(count);
            count++;
            graph.addVertex(sid);
            graph.addVertex(oid);
            graph.addVertex(link);
            graph.addEdge(sid, link);
            graph.addEdge(link, oid);
            Set<List<Integer>> sTriple = entity2Triple.getOrDefault(sid, new HashSet<>());
            sTriple.add(iter);
            entity2Triple.put(sid, sTriple);
            Set<List<Integer>> oTriple = entity2Triple.getOrDefault(oid, new HashSet<>());
            oTriple.add(iter);
            entity2Triple.put(oid, oTriple);
        }
        ConnectivityInspector<Integer, DefaultEdge> inspector = new ConnectivityInspector<>(graph);
        List<Set<Integer>> components = inspector.connectedSets();
        Map<Set<Integer>, Set<List<Integer>>> component2triple = new HashMap<>();
        Map<Set<Integer>, Set<Integer>> component2edp = new HashMap<>();
        Map<Set<Integer>, Integer> component2Size = new HashMap<>();
        for (Set<Integer> comp: components) {
            Set<Integer> edpSet = new TreeSet<>(); //instantiated edps in the component
            Set<List<Integer>> tripleSet = new HashSet<>();
            for (int node: comp) {
                int edp = entity2edp.getOrDefault(node, -1);
                if (edp >= 0) {
                    edpSet.add(edp);
                }
                if (node >= 0) {
                    tripleSet.addAll(entity2Triple.get(node));
                }
            }
            component2edp.put(comp, edpSet);
            component2triple.put(comp, tripleSet);
            component2Size.put(comp, edpSet.size());
        }
        List<Map.Entry<Set<Integer>, Integer>> toBeSortList = new ArrayList<>(component2Size.entrySet());
        Collections.sort(toBeSortList, new Comparator<Map.Entry<Set<Integer>, Integer>>() {
            @Override
            public int compare(Map.Entry<Set<Integer>, Integer> o1, Map.Entry<Set<Integer>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        List<Set<Integer>> resultComp = new ArrayList<>();
        resultComp.add(toBeSortList.get(0).getKey());
        Set<Integer> coveredEDP = new HashSet<>(component2edp.get(toBeSortList.get(0).getKey()));
        Set<Integer> currentEDP = new HashSet<>(component2edp.get(toBeSortList.get(0).getKey()));
        toBeSortList.remove(0);
        while (coveredEDP.size() < edpNum) {
            int maxSize = 0;
            int maxCompIndex = 0;
            Set<Integer> maxComp = null;
            for (int i = 0; i < toBeSortList.size(); i++) {
                Map.Entry<Set<Integer>, Integer> iter = toBeSortList.get(i);
                Set<Integer> tempEDP = component2edp.get(iter.getKey());
                tempEDP.removeAll(currentEDP);
                if (maxSize < tempEDP.size()) {
                    maxSize = tempEDP.size();
                    maxComp = iter.getKey();
                    maxCompIndex = i;
                }
            }
            resultComp.add(maxComp);
            currentEDP = component2edp.get(maxComp);
            coveredEDP.addAll(currentEDP);
            toBeSortList.remove(maxCompIndex);
        }
        try {
            int id = 0;
            for (Set<Integer> comp: resultComp) {
                StringBuilder edpStr = new StringBuilder();
                for (int edp: component2edp.get(comp)) {
                    edpStr.append(edp).append(" ");
                }
                StringBuilder tripleStr = new StringBuilder();
                for (List<Integer> triple: component2triple.get(comp)) {
                    tripleStr.append(triple.get(0)).append(" ").append(triple.get(1)).append(" ").append(triple.get(2)).append(",");
                }
                PrintWriter writer = new PrintWriter(outputFolder + id + ".txt");
                writer.println(edpStr.substring(0, edpStr.length() - 1));
                writer.println(tripleStr.substring(0, tripleStr.length() - 1));
                writer.close();
                id++;
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static boolean buildIndex(String baseFolder, String indexFolder, int edpNum) {
        Map<Integer, Integer> entity2edp = new HashMap<>();
        for (List<Integer> iter: ReadFile.readInteger(PATHS.DBpediaResult + "entity2edp.txt", "\t")) {
            if (iter.get(1) >= edpNum) continue;
            entity2edp.put(iter.get(0), iter.get(1));
        }
        Map<Integer, Set<Integer>> invertedMap = new HashMap<>();
        String[] triples = ReadFile.readString(indexFolder + "0.txt").get(1).split(",");
        try {
            File file = new File(baseFolder);
            if (!file.exists()) {
                file.mkdirs();
            }
            Set<Integer> nodeSet = new HashSet<>();
            String graphFile = baseFolder + "/graph.txt";
            PrintWriter graphWriter = new PrintWriter(graphFile);
            for (String triple: triples) {
                graphWriter.println(triple);
                int sid = Integer.parseInt(triple.split(" ")[0]);
                int oid = Integer.parseInt(triple.split(" ")[2]);
                nodeSet.add(sid);
                nodeSet.add(oid);
                int subjectEDP = entity2edp.getOrDefault(sid, -1);
                if (subjectEDP != -1) {
                    Set<Integer> value = invertedMap.getOrDefault(subjectEDP, new HashSet<>());
                    value.add(sid);
                    invertedMap.put(subjectEDP, value);
                }
                int objectEDP = entity2edp.getOrDefault(oid, -1);
                if (objectEDP != -1) {
                    Set<Integer> value = invertedMap.getOrDefault(objectEDP, new HashSet<>());
                    value.add(oid);
                    invertedMap.put(objectEDP, value);
                }
            }
            graphWriter.close();
            //========subName========
            Map<Integer, Integer> node2Id = new HashMap<>();
            String subNameFile = baseFolder + "/subName.txt";
            PrintWriter subNameWriter = new PrintWriter(subNameFile);
            int count = 0;
            for (int iter: nodeSet) {
                subNameWriter.println(iter);
                node2Id.put(iter, count);
                count++;
            }
            subNameWriter.close();
            //========keyMap========
            String keyMapFile = baseFolder + "/keyMap.txt";
            PrintWriter keyMapWriter = new PrintWriter(keyMapFile);
            for (String iter: ReadFile.readString(indexFolder + "/0.txt").get(0).split(" ")) {
                keyMapWriter.println(iter);
            }
            keyMapWriter.close();
            //========InvertedTable========
            String invertedTableFile = baseFolder + "/invertedTable.txt";
            PrintWriter invTableWriter = new PrintWriter(invertedTableFile);
            for (Map.Entry<Integer, Set<Integer>> iter: invertedMap.entrySet()) {
                Set<Integer> values = iter.getValue();
                StringBuilder content = new StringBuilder();
                for (int v: values) {
                    content.append(node2Id.get(v)).append(" ");
                }
                invTableWriter.println(iter.getKey() + ":" + content.toString().trim());
            }
            invTableWriter.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        return true;
    }

    private static long generateCHL(String baseFolder) {
        long startTime = System.currentTimeMillis();
        try {
            WeightedGraph ww = new WeightedGraph();
            ww.graphIndexRead3(baseFolder); // NOTE HERE
            WeightedPLL w2 = new WeightedPLL();
            w2.pllIndexDeal2(ww, baseFolder);
        }catch (Exception e) {
            e.printStackTrace();
        }
        return System.currentTimeMillis() - startTime;
    }

//    public static void main(String[] args) {
//        String instanceTypeFile = PATHS.DBpediaPath + "instance_types_en.ttl";
//        String relationFile = PATHS.DBpediaPath + "mappingbased-objects_lang=en.ttl";
//        getIRIMap(relationFile, PATHS.DBpediaResult + "IRI2ID.txt", PATHS.DBpediaResult + "Triple.txt");
//        getEntityClass(instanceTypeFile, relationFile, PATHS.DBpediaResult + "IRI2ID.txt", PATHS.DBpediaResult + "Classes.txt", PATHS.DBpediaResult + "Entity2Class.txt");
//        countComponents(PATHS.DBpediaResult + "Triple.txt");

//        getPatternIndex(PATHS.DBpediaResult);

//        getEDPpercent(0.8);
//        testEDPNumberinComponent();
//        getComponentIndex(PATHS.DBpediaResult + "ComponentIndex80/", 2403);
//        buildIndex(PATHS.DBpediaResult + "KeyKGP/0", PATHS.DBpediaResult + "ComponentIndex80/", 2403);
//        generateCHL(PATHS.DBpediaResult + "KeyKGP/0"); //到133:50爆内存了

//        getEDPpercent(0.75);
//        getComponentIndex(PATHS.DBpediaResult + "ComponentIndex75/", 1085);
//        buildIndex(PATHS.DBpediaResult + "KeyKGP75/0", PATHS.DBpediaResult + "ComponentIndex75/", 1085);
//        generateCHL(PATHS.DBpediaResult + "KeyKGP75/0");
//    }
}
