package PCSG.evaluation;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.FSDirectory;
import PCSG.PATHS;
import PCSG.util.DBUtil;
import PCSG.util.IndexUtil;
import PCSG.util.ReadFile;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;

public class generalAnalyzer {

    // generate size-percent-xx.txt files in the result folder, then copy to the excel and process.
    public static void countResultSize(String resultFile, String outputFile) {
        String select = "select dataset_local_id,triple_count from dataset_info_202007 order by dataset_local_id";
        Map<Integer, Integer> size = new HashMap<>();
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement st = connection.prepareStatement(select);
            ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                size.put(resultSet.getInt("dataset_local_id"), resultSet.getInt("triple_count"));
            }
            connection.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        List<List<Integer>> snippetSize = ReadFile.readInteger(resultFile, "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: snippetSize) {
                writer.println(iter.get(0) + "\t" + iter.get(1) + "\t" + size.get(iter.get(0)) + "\t" + ((double)iter.get(1))/((double) size.get(iter.get(0))));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void countResultSizeBase(String outputFile) {
        String select = "select dataset_local_id,triple_count from dataset_info_202007 order by dataset_local_id";
        Map<Integer, Integer> size = new HashMap<>();
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement st = connection.prepareStatement(select);
            ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                size.put(resultSet.getInt("dataset_local_id"), resultSet.getInt("triple_count"));
            }
            connection.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        List<Integer> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0);
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (int dataset: datasets) {
                writer.println(dataset + "\t" + 20 + "\t" + size.get(dataset) + "\t" + ((double) 20)/size.get(dataset));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void countWordResultSize(String resultFile, String outputFile) {
        String select = "select dataset_local_id,triple_count from dataset_info_202007 order by dataset_local_id";
        Map<Integer, Integer> size = new HashMap<>();
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement st = connection.prepareStatement(select);
            ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                size.put(resultSet.getInt("dataset_local_id"), resultSet.getInt("triple_count"));
            }
            connection.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        Map<Integer, Integer> resultSize = new HashMap<>();
        Map<Integer, Integer> result2dataset = new HashMap<>();
        for (List<String> iter: ReadFile.readString(resultFile, "\t")) {
            int id = Integer.parseInt(iter.get(0).split("-")[0]);
            int dataset = Integer.parseInt(iter.get(0).split("-")[1]);
            result2dataset.put(id, dataset);
            int count = Integer.parseInt(iter.get(1));
            resultSize.put(id, count);
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            int pairAmount = ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt").size();
            for (int i = 0; i < pairAmount; i++) {
                if (resultSize.containsKey(i)) {
                    int count = resultSize.get(i);
                    int dataset = result2dataset.get(i);
                    int all = size.get(dataset);
                    writer.println(dataset + "\t" + count + "\t" + all + "\t" + ((double) count)/((double) all));
                }
                else writer.println();
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static void countWordResultSizeBase(String outputFile) {
        String select = "select dataset_local_id,triple_count from dataset_info_202007 order by dataset_local_id";
        Map<Integer, Integer> size = new HashMap<>();
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement st = connection.prepareStatement(select);
            ResultSet resultSet = st.executeQuery();
            while (resultSet.next()) {
                size.put(resultSet.getInt("dataset_local_id"), resultSet.getInt("triple_count"));
            }
            connection.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<String> iter: ReadFile.readString(PATHS.FileBase + "file/QueryPair.txt", "\t")) {
                int dataset = Integer.parseInt(iter.get(0));
                int all = size.get(dataset);
                writer.println(dataset + "\t" + 20 + "\t" + all + "\t" + ((double) 20)/((double) all));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    // generate sub-snip-xx.txt files in the result folder, then copy to the excel and process.
    public static void countSubSnip(String datasetFile, String keyFile, String outputFile) {
        Map<Integer, Integer> subSnip = new HashMap<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(keyFile));
            String line;
            while ((line = reader.readLine()) != null) {
                String id = line.split("\t")[0];
                int dataset = Integer.parseInt(id.split("-")[0]);
                int count = subSnip.getOrDefault(dataset, 0);
                subSnip.put(dataset, count + 1);
            }
            reader.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
        List<List<Integer>> datasets = ReadFile.readInteger(datasetFile, "\t");
        try {
            PrintWriter writer = new PrintWriter(outputFile);
            for (List<Integer> iter: datasets) {
                int dataset = iter.get(0);
                int compCount = IndexUtil.countDocuments(PATHS.ProjectData + "ComponentIndex/" + dataset + "/");
                writer.println(dataset + "\t" + subSnip.get(dataset) + "\t" + compCount);
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void countClassesAndProperties() {
        Connection connection = new DBUtil().conn;
        List<Integer> datasets = ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0);
//        List<Integer> datasets = new ArrayList<>();
//        datasets.add(1);
//        datasets.add(2);
//        datasets.add(3);
//        datasets.add(4);
        String getType = "select type_id from dataset_info_202007 where dataset_local_id = ?";
        String getTriple = "select predicate, object from triple? where dataset_local_id = ?";
        try {
            PreparedStatement getTypeSt = connection.prepareStatement(getType);
            PreparedStatement getTripleSt = connection.prepareStatement(getTriple);
            for (int dataset: datasets) {
                int typeId = 0;
                int cAll = 0;
                int pAll = 0;
                Map<Integer, Integer> cCount = new HashMap<>();
                Map<Integer, Integer> pCount = new HashMap<>();
                getTypeSt.setInt(1, dataset);
                ResultSet resultSet = getTypeSt.executeQuery();
                if (resultSet.next()) {
                    typeId = resultSet.getInt("type_id");
                }
                if (dataset <= 311) {
                    getTripleSt.setInt(1, 2);
                    getTripleSt.setInt(2, dataset);
                }
                else {
                    getTripleSt.setInt(1, 3);
                    getTripleSt.setInt(2, (dataset - 311));
                }
                resultSet = getTripleSt.executeQuery();
                while (resultSet.next()) {
                    int pid = resultSet.getInt("predicate");
                    int oid = resultSet.getInt("object");
                    pAll++;
                    int pcurrent = pCount.getOrDefault(pid, 0);
                    pCount.put(pid, pcurrent + 1);
                    if (pid == typeId) {
                        cAll++;
                        int cCurrent = cCount.getOrDefault(oid, 0);
                        cCount.put(oid, cCurrent + 1);
                    }
                }
                PrintWriter cWriter = new PrintWriter(PATHS.ProjectData + "FrequencyOfClass/" + dataset + ".txt");
                PrintWriter pWriter = new PrintWriter(PATHS.ProjectData + "FrequencyOfProperty/" + dataset + ".txt");
                if (cAll > 0) {
                    List<Map.Entry<Integer, Integer>> cSortList = new ArrayList<>(cCount.entrySet());
                    cSortList.sort(new Comparator<Map.Entry<Integer, Integer>>() {
                        @Override
                        public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                            return o2.getValue() - o1.getValue();
                        }
                    });
                    for (Map.Entry<Integer, Integer> iter: cSortList) {
                        cWriter.println(iter.getKey() + "\t" + iter.getValue() + "\t" + ((double)iter.getValue())/((double) cAll));
                    }
                }
                List<Map.Entry<Integer, Integer>> pSortList = new ArrayList<>(pCount.entrySet());
                pSortList.sort(new Comparator<Map.Entry<Integer, Integer>>() {
                    @Override
                    public int compare(Map.Entry<Integer, Integer> o1, Map.Entry<Integer, Integer> o2) {
                        return o2.getValue() - o1.getValue();
                    }
                });
                for (Map.Entry<Integer, Integer> iter: pSortList) {
                    pWriter.println(iter.getKey() + "\t" + iter.getValue() + "\t" + ((double)iter.getValue())/((double) pAll));
                }
                cWriter.close();
                pWriter.close();
                System.out.println(dataset);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                connection.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static Map<Integer, Integer> All2DistinctCases0() { // BEGIN AT 0 !!
        List<List<Integer>> mapping = ReadFile.readInteger(PATHS.FileBase + "file/TKDERecordsToDistinctId.txt", "\t");
        Map<Integer, Integer> result = new HashMap<>();
        for (List<Integer> iter: mapping) {
            result.put(iter.get(0) - 1, iter.get(1) - 1);
        }
        return result;
    }

    public static void transferRecords(String shortFile, String longFile) {
        List<String> currentRecord = ReadFile.readString(shortFile);
        Map<Integer, Integer> idMap = All2DistinctCases0();
        try {
            PrintWriter writer = new PrintWriter(longFile);
            for (int i = 0; i < idMap.size(); i++) {
                writer.println(currentRecord.get(idMap.get(i)));
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void countPatternCompressionRate() {
        List<Integer> datasets = ReadFile.readInteger(PATHS.ProjectData + "Ablation/datasetAll.txt");
        try {
            PrintWriter writer = new PrintWriter(PATHS.ProjectData + "Ablation/pattern-compression-all.txt");
            for (int dataset: datasets) {
                String componentFolder = PATHS.ProjectData + "ComponentIndex/" + dataset + "/";
                IndexReader componentReader = DirectoryReader.open(FSDirectory.open(Paths.get(componentFolder)));
                System.out.println(dataset + "\t" + componentReader.maxDoc());
//                if (componentReader.maxDoc() == 1) {
//                    continue;
//                }
                int alledp = 0;
                int alllp = 0;
                for (int i = 0; i < componentReader.maxDoc(); i++) {
                    Document doc = componentReader.document(i);
                    alledp += doc.get("edp").split(" ").length;
                    alllp += doc.get("lp").split(" ").length;
                }
                String edpIndex = PATHS.ProjectData + "EDPIndex/" + dataset + "/";
                int distinctEdp = IndexUtil.countDocuments(edpIndex);
                String lpIndex = PATHS.ProjectData + "LPIndex/" + dataset + "/";
                int distinctLp = IndexUtil.countDocuments(lpIndex);
                writer.println(dataset + "\t" + distinctEdp + "\t" + alledp + "\t" + distinctLp + "\t" + alllp + "\t" + (distinctEdp + distinctLp) + "\t" + (alledp + alllp) + "\t" + ((double)(distinctEdp + distinctLp))/((double) (alledp + alllp)));
//                System.out.println(dataset);
            }
            writer.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static int getTypeID(int dataset){
        int typeID = 0;
        String select = "select type_id from dataset_info_202007 where dataset_local_id = " + dataset;
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement selectStatement = connection.prepareStatement(select);
            ResultSet resultSet = selectStatement.executeQuery();
            resultSet.next();
            typeID = resultSet.getInt("type_id");
            connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }
        return typeID;
    }

    private static void countDatasetInfo(String outputFile) {
        String getTriple = "SELECT COUNT(*) FROM triple? WHERE dataset_local_id = ?";
        String getProperty = "SELECT COUNT(DISTINCT(predicate)) FROM triple? WHERE dataset_local_id = ?";
        String getClasses = "SELECT COUNT(DISTINCT(object)) FROM triple? WHERE dataset_local_id = ? AND predicate = ?";
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement tripleSt = connection.prepareStatement(getTriple);
            PreparedStatement propertySt = connection.prepareStatement(getProperty);
            PreparedStatement classSt = connection.prepareStatement(getClasses);
            PrintWriter writer = new PrintWriter(outputFile);
            for (int dataset: ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0)) {
                int tripleNum = 0;
                int propertyNum = 0;
                int classNum = 0;
                if (dataset <= 311) {
                    tripleSt.setInt(1, 2);
                    tripleSt.setInt(2, dataset);
                    ResultSet resultSet = tripleSt.executeQuery();
                    resultSet.next();
                    tripleNum = resultSet.getInt(1);

                    propertySt.setInt(1, 2);
                    propertySt.setInt(2, dataset);
                    ResultSet resultSet1 = propertySt.executeQuery();
                    resultSet1.next();
                    propertyNum = resultSet1.getInt(1);

                    int typeId = getTypeID(dataset);
                    if (typeId != 0) {
                        classSt.setInt(1, 2);
                        classSt.setInt(2, dataset);
                        classSt.setInt(3, typeId);
                        ResultSet resultSet2 = classSt.executeQuery();
                        resultSet2.next();
                        classNum = resultSet2.getInt(1);
                    }
                }
                else {
                    tripleSt.setInt(1, 3);
                    tripleSt.setInt(2, dataset-311);
                    ResultSet resultSet = tripleSt.executeQuery();
                    resultSet.next();
                    tripleNum = resultSet.getInt(1);

                    propertySt.setInt(1, 3);
                    propertySt.setInt(2, dataset-311);
                    ResultSet resultSet1 = propertySt.executeQuery();
                    resultSet1.next();
                    propertyNum = resultSet1.getInt(1);

                    int typeId = getTypeID(dataset);
                    if (typeId != 0) {
                        classSt.setInt(1, 3);
                        classSt.setInt(2, dataset-311);
                        classSt.setInt(3, typeId);
                        ResultSet resultSet2 = classSt.executeQuery();
                        resultSet2.next();
                        classNum = resultSet2.getInt(1);
                    }
                }
                int edpNum = IndexUtil.countDocuments(PATHS.ProjectData + "EDPIndex/" + dataset + "/");
                int lpNum = IndexUtil.countDocuments(PATHS.ProjectData + "LPIndex/" + dataset + "/");
                writer.println(dataset + "\t" + tripleNum + "\t" + classNum + "\t" + propertyNum + "\t" + edpNum + "\t" + lpNum);
                System.out.println(dataset);
            }
            writer.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void countEntities(String outputFile) {
        String getLiteral = "SELECT id,is_literal FROM uri_label_id? WHERE dataset_local_id = ?";
        String getTriple = "SELECT subject,predicate,object FROM triple? WHERE dataset_local_id = ?";
        try {
            Connection connection = new DBUtil().conn;
            PreparedStatement literalSt = connection.prepareStatement(getLiteral);
            PreparedStatement tripleSt = connection.prepareStatement(getTriple);
            PrintWriter writer = new PrintWriter(outputFile);
            for (int dataset: ReadFile.readInteger(PATHS.FileBase + "file/dataset.txt", "\t").get(0)) {
                Set<Integer> literalSet = new HashSet<>();
                Set<Integer> entitySet = new HashSet<>();
                int typeId = getTypeID(dataset);
                if (dataset <= 311) {
                    literalSt.setInt(1, 2);
                    literalSt.setInt(2, dataset);
                    ResultSet resultSet = literalSt.executeQuery();
                    while (resultSet.next()) {
                        if (resultSet.getInt("is_literal") == 1) {
                            literalSet.add(resultSet.getInt("id"));
                        }
                    }
                    resultSet.close();

                    tripleSt.setInt(1, 2);
                    tripleSt.setInt(2, dataset);
                    ResultSet resultSet1 = tripleSt.executeQuery();
                    while (resultSet1.next()) {
                        entitySet.add(resultSet1.getInt("subject"));
                        int object = resultSet1.getInt("object");
                        if (resultSet1.getInt("predicate") != typeId && !literalSet.contains(object)) {
                            entitySet.add(object);
                        }
                    }
                    resultSet1.close();
                }
                else {
                    literalSt.setInt(1, 3);
                    literalSt.setInt(2, dataset-311);
                    ResultSet resultSet = literalSt.executeQuery();
                    while (resultSet.next()) {
                        if (resultSet.getInt("is_literal") == 1) {
                            literalSet.add(resultSet.getInt("id"));
                        }
                    }
                    resultSet.close();

                    tripleSt.setInt(1, 3);
                    tripleSt.setInt(2, dataset-311);
                    ResultSet resultSet1 = tripleSt.executeQuery();
                    while (resultSet1.next()) {
                        entitySet.add(resultSet1.getInt("subject"));
                        int object = resultSet1.getInt("object");
                        if (resultSet1.getInt("predicate") != typeId && !literalSet.contains(object)) {
                            entitySet.add(object);
                        }
                    }
                    resultSet1.close();
                }
                writer.println(dataset + "\t" + entitySet.size());
                System.out.println(dataset);
            }
            writer.close();
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

//    public static void main(String[] args) {
//        countResultSize(PATHS.ProjectData + "SnippetResultCount80.txt", PATHS.PaperResult + "size-percent-80.txt");
//        countResultSize(PATHS.ProjectData + "SnippetResultCount90.txt", PATHS.PaperResult + "size-percent-90.txt");
//        countResultSize(PATHS.ProjectData + "SnippetResultCount.txt", PATHS.PaperResult + "size-percent.txt");
//        countResultSizeBase(PATHS.PaperResult + "size-percent-base.txt");

//        countWordResultSize(PATHS.ProjectData + "SnippetWordResultCount.txt", PATHS.PaperResult + "size-word-percent.txt");
//        transferRecords(PATHS.PaperResult + "size-word-percent.txt", PATHS.PaperResult + "size-word-percent-record.txt");
//        countWordResultSize(PATHS.ProjectData + "SnippetWordResultCount90-2.txt", PATHS.PaperResult + "size-word-percent90-2.txt");
//        transferRecords(PATHS.PaperResult + "size-word-percent90-2.txt", PATHS.PaperResult + "size-word-percent-record90-2.txt");
//        countWordResultSize(PATHS.ProjectData + "SnippetWordResultCount80-2.txt", PATHS.PaperResult + "size-word-percent80-2.txt");
//        transferRecords(PATHS.PaperResult + "size-word-percent80-2.txt", PATHS.PaperResult + "size-word-percent-record80-2.txt");
//        countWordResultSizeBase(PATHS.PaperResult + "size-word-percent-base.txt");
//        transferRecords(PATHS.PaperResult + "size-word-percent-base.txt", PATHS.PaperResult + "size-word-percent-base-record.txt");

//        countSubSnip(PATHS.ProjectData + "SnippetResultCount80.txt", PATHS.FileBase + "file/Keys80.txt", PATHS.PaperResult + "sub-snip-80.txt");
//        countSubSnip(PATHS.ProjectData + "SnippetResultCount90.txt", PATHS.FileBase + "file/Keys90.txt", PATHS.PaperResult + "sub-snip-90.txt");
//        countSubSnip(PATHS.ProjectData + "SnippetResultCount.txt", PATHS.FileBase + "file/Keys.txt", PATHS.PaperResult + "sub-snip.txt");

//        countClassesAndProperties();
//        countPatternCompressionRate();
//        countDatasetInfo(PATHS.PaperResult + "dataset-stat.txt");
//        transferRecords(PATHS.PaperResult + "dataset-stat.txt", PATHS.PaperResult + "dataset-stat-record.txt");
//    }
}
