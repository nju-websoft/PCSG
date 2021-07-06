package PCSG.beans;

import DOGST.AnsTree;

public class KeyKGResultBean {
    public String id;
    public long runtime;
    public AnsTree resultTree;

    public KeyKGResultBean(String id, AnsTree result, long runtime) {
        this.id = id;
        this.resultTree = result;
        this.runtime = runtime;
    }
}
