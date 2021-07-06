package PCSG.beans;

public class KSDTriple extends Triple implements Comparable<KSDTriple> {

    public double weight = 0; //for sorting weightedTriples
    public double kwsW = 0, prpW = 0, clsW = 0, outW = 0, inW = 0;

    public void setW() {
        weight = 2*kwsW + prpW + clsW + outW + inW;
    }

    public int compareTo(KSDTriple object) {
        return Double.compare(object.weight, this.weight);//descending order
    }

}
