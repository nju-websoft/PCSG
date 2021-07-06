package PCSG.abstat;

import PCSG.util.ReadFile;

import java.io.PrintWriter;
import java.util.*;

public class generatePattern {
    private static void testDistinctClassLabel(String instanceTypeFile) {
        Set<String> wholeClass = new HashSet<>();
        Set<String> briefClass = new HashSet<>();
        for (String line: ReadFile.readString(instanceTypeFile)) {
            if (!line.startsWith("<")) {
                continue;
            }
            String object = line.split("\\s+")[2];
            String uri = object.substring(1, object.length() - 1);
            wholeClass.add(uri);
            briefClass.add(uri.substring(uri.lastIndexOf("/") + 1));
        }
        System.out.println(wholeClass.size());
        System.out.println(briefClass.size());
    }

    private static void testDistinctPropertyLabel(String relationFile) {
        Set<String> wholeProp = new HashSet<>();
        Set<String> briefProp = new HashSet<>();
        for (String line: ReadFile.readString(relationFile)) {
            if (!line.startsWith("<")) {
                continue;
            }
            String object = line.split("\\s+")[1];
            String uri = object.substring(1, object.length() - 1);
            wholeProp.add(uri);
            briefProp.add(uri.substring(uri.lastIndexOf("/") + 1));
        }
        System.out.println(wholeProp.size());
        System.out.println(briefProp.size());
    }

    private static void testDistinctLocalName(String relationFile) {
        Set<String> uriSet = new HashSet<>();
        Set<String> localNameSet = new HashSet<>();
        Map<String, String> localName2uri = new HashMap<>();
        for (String line: ReadFile.readString(relationFile)) {
            if (!line.startsWith("<")) {
                continue;
            }
            String[] spo = line.split("\\s+");
            String subject = spo[0].substring(1, spo[0].length() - 1);
            String object = spo[2].substring(1, spo[2].length() - 1);
            String subjectName = subject.substring(subject.lastIndexOf("/") + 1);
            String objectName = object.substring(object.lastIndexOf("/") + 1);
            if (localName2uri.containsKey(subjectName) && !uriSet.contains(subject)) {
                System.out.println("subject: ");
                System.out.println(subjectName);
                System.out.println(localName2uri.get(subjectName));
                System.out.println(subject);
                System.out.println(line);
                return;
            }
            if (localName2uri.containsKey(objectName) && !uriSet.contains(object)) {
                System.out.println("object: ");
                System.out.println(objectName);
                System.out.println(localName2uri.get(objectName));
                System.out.println(object);
                System.out.println(line);
                return;
            }
            localName2uri.put(subjectName, subject);
            localName2uri.put(objectName, object);
            uriSet.add(subject);
            uriSet.add(object);
//            uriSet.add(subject);
//            uriSet.add(object);
//            localNameSet.add(subject.substring(subject.lastIndexOf("/") + 1));
//            localNameSet.add(object.substring(object.lastIndexOf("/") + 1));
        }
//        System.out.println(uriSet.size());
//        System.out.println(localNameSet.size());
    }

    private static void testIfAlluri(String relationFile) {
        for (String line: ReadFile.readString(relationFile)) {
            String[] spo = line.split("\\s+");
            if (!spo[0].startsWith("<") || !spo[1].startsWith("<") || !spo[2].startsWith("<")) {
                System.out.println(line);
            }
        }
    }

    private static void getABSTATPatterns(String instanceTypeFile, String mappingBasedFile, String outputFile) {
        Map<String, Integer> type2id = new HashMap<>();
        Map<String, Integer> entity2type = new HashMap<>();
        Map<Integer, Integer> type2count = new HashMap<>();
        List<String> typeList = new ArrayList<>();
        type2id.put("owl#Thing", 0); // as default
        typeList.add("owl#Thing");
        for (String line: ReadFile.readString(instanceTypeFile)) {
            if (!line.startsWith("<")) continue;
            String classIRI = line.split("\\s+")[2];
            String type = classIRI.substring(classIRI.lastIndexOf("/") + 1, classIRI.length() - 1);
            if (!type2id.containsKey(type)) {
                type2id.put(type, typeList.size());
                typeList.add(type);
            }
            String subjectIRI = line.split("\\s+")[0];
            String name = subjectIRI.substring(subjectIRI.lastIndexOf("/") + 1, subjectIRI.length() - 1);
            int typeId = type2id.get(type);
            entity2type.put(name, typeId);
            type2count.put(typeId, type2count.getOrDefault(typeId, 0) + 1);
        }
        Map<List<Integer>, Integer> pattern2count = new HashMap<>(); // minimal type -> count
        Map<String, Integer> relation2id = new HashMap<>();
        Map<Integer, Integer> relation2count = new HashMap<>();
        List<String> relationList = new ArrayList<>();
        for (String line: ReadFile.readString(mappingBasedFile)) {
            if (!line.startsWith("<")) continue;
            String subjectIRI = line.split("\\s+")[0];
            String subject = subjectIRI.substring(subjectIRI.lastIndexOf("/") + 1, subjectIRI.length() - 1);
            String relationIRI = line.split("\\s+")[1];
            String relation = relationIRI.substring(relationIRI.lastIndexOf("/") + 1, relationIRI.length() - 1);
            if (!relation2id.containsKey(relation)) {
                relation2id.put(relation, relationList.size());
                relationList.add(relation);
            }
            int relationId = relation2id.get(relation);
            relation2count.put(relationId, relation2count.getOrDefault(relationId, 0) + 1);
            String objectIRI = line.split("\\s+")[2];
            String object = objectIRI.substring(objectIRI.lastIndexOf("/") + 1, objectIRI.length() - 1);
            List<Integer> pattern = new ArrayList<>(Arrays.asList(entity2type.getOrDefault(subject, 0), relationId, entity2type.getOrDefault(object, 0)));
            int count = pattern2count.getOrDefault(pattern, 0);
            pattern2count.put(pattern, count + 1);
        }
        List<Map.Entry<List<Integer>, Integer>> entryList = new ArrayList<>(pattern2count.entrySet());
        entryList.sort(new Comparator<Map.Entry<List<Integer>, Integer>>() {
            @Override
            public int compare(Map.Entry<List<Integer>, Integer> o1, Map.Entry<List<Integer>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (Map.Entry<List<Integer>, Integer> iter: entryList) {
                List<Integer> pattern = iter.getKey();
                writer.println(typeList.get(pattern.get(0)) + "\t" + type2count.get(pattern.get(0)) + "\t" + relationList.get(pattern.get(1)) + "\t" + relation2count.get(pattern.get(1)) + "\t" + typeList.get(pattern.get(2)) + "\t" + type2count.get(pattern.get(2)) + "\t" + iter.getValue());
            }
            writer.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void getPatterns(String instanceTypeFile, String mappingBasedFile, String edpOutputFile, String lpOutputFile) {
        long start = System.currentTimeMillis();
        Map<String, Integer> type2id = new HashMap<>();
        Map<String, Integer> entity2type = new HashMap<>();
        List<String> typeList = new ArrayList<>();
        type2id.put("owl#Thing", 0); // as default
        typeList.add("owl#Thing");
        for (String line: ReadFile.readString(instanceTypeFile)) {
            if (!line.startsWith("<")) continue;
            String classIRI = line.split("\\s+")[2];
            String type = classIRI.substring(classIRI.lastIndexOf("/") + 1, classIRI.length() - 1);
            if (!type2id.containsKey(type)) {
                type2id.put(type, typeList.size());
                typeList.add(type);
            }
            String subjectIRI = line.split("\\s+")[0];
            String name = subjectIRI.substring(subjectIRI.lastIndexOf("/") + 1, subjectIRI.length() - 1);
            entity2type.put(name, type2id.get(type));
        }
        ///////////////////////////////////
        List<List<Integer>> tripleList = new ArrayList<>();
        Map<Integer, List<Set<Integer>>> entity2edp = new HashMap<>(); // value.get(0): class, value.get(1): out property as subject, value.get(2): in property as object
        Map<String, Integer> entity2id = new HashMap<>();
        List<String> entityList = new ArrayList<>();
        Map<String, Integer> relation2id = new HashMap<>();
        List<String> relationList = new ArrayList<>();
        for (String line: ReadFile.readString(mappingBasedFile)) {
            if (!line.startsWith("<")) continue;
            String subjectIRI = line.split("\\s+")[0];
            String subject = subjectIRI.substring(subjectIRI.lastIndexOf("/") + 1, subjectIRI.length() - 1);
            if (!entity2id.containsKey(subject)) {
                entity2id.put(subject, entityList.size());
                entityList.add(subject);
                List<Set<Integer>> initList = new ArrayList<>();
                initList.add(new HashSet<>());
                initList.add(new HashSet<>());
                initList.add(new HashSet<>());
                initList.get(0).add(entity2type.getOrDefault(subject, 0));
                entity2edp.put(entityList.size() - 1, initList);
            }
            String relationIRI = line.split("\\s+")[1];
            String relation = relationIRI.substring(relationIRI.lastIndexOf("/") + 1, relationIRI.length() - 1);
            if (!relation2id.containsKey(relation)) {
                relation2id.put(relation, relationList.size());
                relationList.add(relation);
            }
            String objectIRI = line.split("\\s+")[2];
            String object = objectIRI.substring(objectIRI.lastIndexOf("/") + 1, objectIRI.length() - 1);
            if (!entity2id.containsKey(object)) {
                entity2id.put(object, entityList.size());
                entityList.add(object);
                List<Set<Integer>> initList = new ArrayList<>();
                initList.add(new HashSet<>());
                initList.add(new HashSet<>());
                initList.add(new HashSet<>());
                initList.get(0).add(entity2type.getOrDefault(subject, 0));
                entity2edp.put(entityList.size() - 1, initList);
            }
            int subjectId = entity2id.get(subject);
            int relationId = relation2id.get(relation);
            int objectId = entity2id.get(object);
            entity2edp.get(subjectId).get(1).add(relationId);
            entity2edp.get(objectId).get(2).add(relationId);
            tripleList.add(new ArrayList<>(Arrays.asList(subjectId, relationId, objectId)));
        }
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        Map<List<Set<Integer>>, Integer> edp2count = new HashMap<>();
        for (int i = 0; i < entityList.size(); i++) {
            List<Set<Integer>> edp = entity2edp.get(i);
            edp2count.put(edp, edp2count.getOrDefault(edp, 0) + 1);
        }
        List<Map.Entry<List<Set<Integer>>, Integer>> edpCountList = new ArrayList<>(edp2count.entrySet());
        edpCountList.sort(new Comparator<Map.Entry<List<Set<Integer>>, Integer>> () {
            @Override
            public int compare(Map.Entry<List<Set<Integer>>, Integer> o1, Map.Entry<List<Set<Integer>>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        Map<List<Set<Integer>>, Integer> edp2id = new HashMap<>();
        try {
            PrintWriter edpWriter = new PrintWriter(edpOutputFile);
            int id = 0;
            for (Map.Entry<List<Set<Integer>>, Integer> iter: edpCountList) {
                List<Set<Integer>> edp = iter.getKey();
                edp2id.put(edp, id);
                id++;
                for (int i: edp.get(0)) {
                    edpWriter.print(typeList.get(i) + "\t");
                }
                StringBuilder outRelation = new StringBuilder();
                List<Integer> temp = new ArrayList<>(edp.get(1));
                if (!temp.isEmpty()) {
                    Collections.sort(temp);
                    for (int i: temp) {
                        outRelation.append(relationList.get(i)).append(",");
                    }
                    edpWriter.print(outRelation.substring(0, outRelation.length() - 1) + "\t");
                }
                else {
                    edpWriter.print("\t");
                }
                StringBuilder inRelation = new StringBuilder();
                temp = new ArrayList<>(edp.get(2));
                if (!temp.isEmpty()) {
                    Collections.sort(temp);
                    for (int i: temp) {
                        inRelation.append(relationList.get(i)).append(",");
                    }
                    edpWriter.print(inRelation.substring(0, inRelation.length() - 1) + "\t");
                }
                else {
                    edpWriter.print("\t");
                }
                edpWriter.println(iter.getValue());
            }
            edpWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        Map<List<Integer>, Integer> lp2count = new HashMap<>();
        for (List<Integer> triple: tripleList) {
            int sid = triple.get(0);
            int pid = triple.get(1);
            int oid = triple.get(2);
            List<Integer> lp = new ArrayList<>(Arrays.asList(edp2id.get(entity2edp.get(sid)), pid, edp2id.get(entity2edp.get(oid))));
            lp2count.put(lp, lp2count.getOrDefault(lp, 0) + 1);
        }
        ///////////////////////////////////
        List<Map.Entry<List<Integer>, Integer>> lpCountList = new ArrayList<>(lp2count.entrySet());
        lpCountList.sort(new Comparator<Map.Entry<List<Integer>, Integer>>() {
            @Override
            public int compare(Map.Entry<List<Integer>, Integer> o1, Map.Entry<List<Integer>, Integer> o2) {
                return o2.getValue() - o1.getValue();
            }
        });
        try {
            PrintWriter lpWriter = new PrintWriter(lpOutputFile);
            for (Map.Entry<List<Integer>, Integer> iter: lpCountList) {
                List<Integer> lp = iter.getKey();
                lpWriter.println(lp.get(0) + "\t" + relationList.get(lp.get(1)) + "\t" + lp.get(2) + "\t" + iter.getValue());
            }
            lpWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(System.currentTimeMillis() - start);
    }

//    public static void main(String[] args) {
//        String typeFile = PATHS.DBpediaPath + "instance_types_en.ttl";
//        String relationFile = PATHS.DBpediaPath + "mappingbased-objects_lang=en.ttl";

//        testDistinctClassLabel(typeFile);
//        testDistinctPropertyLabel(relationFile);
//        getABSTATPatterns(typeFile, relationFile, PATHS.DBpediaPath + "abstat.txt");
//        getPatterns(typeFile, relationFile, PATHS.DBpediaPath + "edp.txt", PATHS.DBpediaPath + "lp.txt");

//        testDistinctLocalName(relationFile);
//        testIfAlluri(relationFile);
//    }
}
