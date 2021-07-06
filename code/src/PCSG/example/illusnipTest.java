package PCSG.example;

import org.jgrapht.alg.scoring.PageRank;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.Multigraph;
import PCSG.beans.OPTTriple;
import PCSG.util.ReadFile;

import java.util.*;


public class illusnipTest {

    private int MAX_SIZE = 20;
    private int typeId;
    private List<OPTTriple> heap = new ArrayList<>();
    private Set<Integer> literalSet = new HashSet<>();
    private Map<Integer, Integer> propertyCount = new HashMap<>(); //property -> count
    private Map<Integer, Integer>classCount = new HashMap<>(); //class -> count
    private Multigraph<Integer, DefaultEdge> ERgraph = new Multigraph<>(DefaultEdge.class);//nodes: entity; edges: relation
    PageRank<Integer, DefaultEdge> pageRank;
    double MaxPR;
    private static int T, C; //triple_count

    public Set<OPTTriple> result = new HashSet<>();//anytime snippet
    public Set<OPTTriple> currentSnippet = new HashSet<>();

    private void createTriples(String folder) {
        typeId = DatasetIndexer.getTypeID(folder + "label.txt");
        literalSet = DatasetIndexer.getLiteralSet(folder + "label.txt");
        for (List<Integer> iter: ReadFile.readInteger(folder + "dataset.txt", "\t")) {
            int s = iter.get(0);
            int p = iter.get(1);
            int o = iter.get(2);
            OPTTriple triple = new OPTTriple();
            triple.setSid(s);
            triple.setPid(p);
            triple.setOid(o);
            heap.add(triple);
            int pCount = propertyCount.getOrDefault(p, 0);
            propertyCount.put(p, pCount+1);//property
            if (p == typeId){//object is class
                int cCount = classCount.getOrDefault(o, 0);
                classCount.put(o, cCount+1);
                ERgraph.addVertex(s);
                continue;
            }
            if (literalSet.contains(o) || s == o) {
                ERgraph.addVertex(s);
                continue;
            }
            ERgraph.addVertex(s);
            ERgraph.addVertex(o);
            ERgraph.addEdge(s, o);
        }
        T = heap.size();
        if (typeId != 0){
            for (int iter: classCount.keySet()){
                C += classCount.get(iter);
            }
        }
    }

    private void setInitialWeight(){
        try {
            pageRank = new PageRank<>(ERgraph, 0.85);
            MaxPR = Collections.max(pageRank.getScores().values());
            for (OPTTriple iter: heap){
                int s = iter.getSid();
                int p = iter.getPid();
                int o = iter.getOid();
                if (p == typeId) {
                    iter.Fam = pageRank.getVertexScore(s)/MaxPR;//========Weight of Familiarity========
                    iter.Cov = Math.log(classCount.get(o) + 1)/Math.log(C + 1);
                } else {
                    if (literalSet.contains(o)) {
                        iter.Fam = pageRank.getVertexScore(s)/MaxPR;
                    } else {
                        iter.Fam = (pageRank.getVertexScore(s) + pageRank.getVertexScore(o))/(2 * MaxPR);
                    }//========Weight of Familiarity========
                    iter.Cov = Math.log(propertyCount.get(p) + 1)/Math.log(T + 1);
                }//TYPE is excluded from properties
                iter.setW();
            }
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    private void getResultSnippet() {
        Collections.sort(heap);//sorting triple list

        HashMap<Integer, HashSet<Integer>> entity2Triple = new HashMap<>();
        for (int i = 0; i < heap.size(); i++) {
            OPTTriple iter = heap.get(i);
            int s = iter.getSid();
            int p = iter.getPid();
            int o = iter.getOid();
            HashSet<Integer> temp = entity2Triple.getOrDefault(s, new HashSet<>());
            temp.add(i);
            entity2Triple.put(s, temp);
            if (p != typeId && !literalSet.contains(o)) {
                HashSet<Integer> temp2 = entity2Triple.getOrDefault(o, new HashSet<>());
                temp2.add(i);
                entity2Triple.put(o, temp2);
            }
        }
        //finish map: entity ID -> triples containing it
        double maxScore = 0;
        for (int i = 0; i < heap.size(); i++) {
            if (Thread.interrupted()) {
                return;
            }
            //heap is fixed, and can't be modified. Elements can only be CLONED to neighbor set.
            OPTTriple init = heap.get(i);
            currentSnippet = new HashSet<>();
            double currentScore = 0;
            int currentSize = 0;
            HashSet<Integer> coveredProperty = new HashSet<>();
            HashSet<Integer> coveredClass = new HashSet<>();
            HashSet<Integer> coveredEntity = new HashSet<>();
            ArrayList<OPTTriple> neighbors = new ArrayList<>();
            neighbors.add(init.clone());
            HashSet<Integer> visited = new HashSet<>();//triples that have been added to snippet
            visited.add(i);
            while (currentSize < MAX_SIZE && !neighbors.isEmpty()) {
                //========sort========
                Collections.sort(neighbors);
                //========get top========
                OPTTriple top = neighbors.get(0);
                if (top.weight == 0) {
                    break;
                }
                currentSnippet.add(top);
                currentScore += top.weight;
                currentSize ++;
                //========renew sets========
                neighbors.remove(0);
                int s = top.getSid();
                int p = top.getPid();
                int o = top.getOid();
                if (top.Fam > 0) {
                    if (p == typeId || literalSet.contains(o)) {
                        coveredEntity.add(s);
                    } else {
                        coveredEntity.add(s);
                        coveredEntity.add(o);
                    }
                }
                if (top.Cov > 0) {
                    if (p == typeId) {
                        coveredClass.add(o);
                    } else {
                        coveredProperty.add(p);
                    }
                }
                //========add neighbors========
                HashSet<Integer> temp = new HashSet<>();
                temp.addAll(entity2Triple.get(s));
                temp.addAll(entity2Triple.getOrDefault(o, new HashSet<>()));
                for (int nbIter: temp) {
                    if (!visited.contains(nbIter)) {
                        neighbors.add(heap.get(nbIter).clone());
                        visited.add(nbIter);
                    }
                }
                //========change weight of all neighbors========
                for (OPTTriple iter: neighbors) {
                    if (iter.weight > 0) {
                        int s1 = iter.getSid();
                        int p1 = iter.getPid();
                        int o1 = iter.getOid();
                        if (iter.Fam > 0) {
                            if ((p1 == typeId || literalSet.contains(o1)) && coveredEntity.contains(s1)) {
                                iter.Fam = 0;
                            } else if (coveredEntity.contains(s1) && coveredEntity.contains(o1)) {
                                iter.Fam = 0;
                            } else if (coveredEntity.contains(o1)) {
                                iter.Fam = pageRank.getVertexScore(s1)/MaxPR;
                            } else if (coveredEntity.contains(s1)) {
                                iter.Fam = pageRank.getVertexScore(o1)/MaxPR;
                            }
                        }
                        if (iter.Cov > 0 && (coveredProperty.contains(p1) || coveredClass.contains(o1))) {
                            iter.Cov = 0;
                        }
                        iter.setW();
                    }
                }
            }
            if (maxScore < currentScore) {
                maxScore = currentScore;
                result = currentSnippet;
            }
        }
    }

    private void getResult(String folder) {
        createTriples(folder);
        setInitialWeight();
        getResultSnippet();
    }

    public static void main(String[] args) {
        String folder = "C:/Users/Desktop/example/"; // change to your local folder where there are graph.txt and label.txt files.
        illusnipTest test = new illusnipTest();
        test.getResult(folder);
        System.out.println(test.result);
    }
}
