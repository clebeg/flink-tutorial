package bigdata.clebeg.cn.lru;

public class DTreeNode {
    public String key;
    public Integer value;
    public DTreeNode pre;
    public DTreeNode next;

    public DTreeNode() {
    }

    public DTreeNode(String key, Integer value, DTreeNode pre, DTreeNode next) {
        this.key = key;
        this.value = value;
        this.pre = pre;
        this.next = next;
    }
}
