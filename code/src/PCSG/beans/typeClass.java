package PCSG.beans;

public class typeClass {
	private String name;
	private double count = 0;//实例数量

	public void setName(String s) {
		name = s;
	}
	
	public String getName() {
		return name;
	}
	
	public void add() {
		count++;
	}
	
	public double getCount() {
		return count;
	}

}
