package PCSG.beans;

public class ResultBean {
    public int id;
    public int dataset;
    public int component;
    public String keyword;
    public String snippet;
    public long runningTime;

    public ResultBean(int id, int dataset, String keyword, String snippet, long runningTime) { // KSD
        this.id = id;
        this.dataset = dataset;
        this.keyword = keyword;
        this.snippet = snippet;
        this.runningTime = runningTime;
    }

    public ResultBean(int dataset, String snippet, long runningTime) { // IlluSnip
        this.dataset = dataset;
        this.snippet = snippet;
        this.runningTime = runningTime;
    }

    public ResultBean(int id, int dataset, int component, String keyword, String snippet, long runningTime) { // PrunedDP
        this.id = id;
        this.dataset = dataset;
        this.component = component;
        this.keyword = keyword;
        this.snippet = snippet;
        this.runningTime = runningTime;
    }
}
