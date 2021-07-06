package PCSG.beans;

public class OPTKeyTriple extends Triple implements Comparable<OPTKeyTriple> {

    public double weight = 0; //for sorting weightedTriples
    public double Fam = 0, Cov = 0, Kws = 0;

    public void setW() { //hyper-parameters: 0.2, 0.8, 1.0
        weight = 0.2*Fam + 0.8*Cov + Kws;
    }

    public int compareTo(OPTKeyTriple object) {
        return Double.compare(object.weight, this.weight);//descending order
    }

    public OPTKeyTriple clone() {
        OPTKeyTriple triple = new OPTKeyTriple();
        triple.setSid(this.getSid());
        triple.setPid(this.getPid());
        triple.setOid(this.getOid());
        triple.weight = this.weight;
        triple.Fam = this.Fam;
        triple.Cov = this.Cov;
        triple.Kws = this.Kws;
        return triple;
    }

    public String toString() {
        return (this.getSid() + " " + this.getPid() + " " + this.getOid());
    }
}
