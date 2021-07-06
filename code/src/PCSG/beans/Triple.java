package PCSG.beans;

public class Triple {
    private int subject, predicate, object;

    public Triple() {
    }

    public Triple(int s, int p, int o) {
        subject = s;
        predicate = p;
        object = o;
    }

    public void setSid(int arg){subject = arg;}
    public int getSid(){return subject;}

    public void setPid(int arg){predicate = arg;}
    public int getPid(){return predicate;}

    public void setOid(int arg){object = arg;}
    public int getOid(){return object;}
}
