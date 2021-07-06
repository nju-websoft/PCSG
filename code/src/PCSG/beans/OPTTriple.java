package PCSG.beans;

public class OPTTriple extends Triple implements Comparable<OPTTriple> {

    public double weight = 0; //for sorting weightedTriples
    public double Fam = 0, Cov = 0;

    public void setW() {//hyper-parameters: 0.2, 0.8
        weight = 0.2*Fam + 0.8*Cov;
    }

    public int compareTo(OPTTriple object) {
        return Double.compare(object.weight, this.weight); //descending order
    }

    public OPTTriple clone() {
        OPTTriple triple = new OPTTriple();
        triple.setSid(this.getSid());
        triple.setPid(this.getPid());
        triple.setOid(this.getOid());
        triple.weight = this.weight;
        triple.Fam = this.Fam;
        triple.Cov = this.Cov;
        return triple;
    }

    public String toString() {
        return (this.getSid() + " " + this.getPid() + " " + this.getOid());
    }
}
