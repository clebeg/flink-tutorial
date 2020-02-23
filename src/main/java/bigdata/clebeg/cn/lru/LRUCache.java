package bigdata.clebeg.cn.lru;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于双向链表 和  HashMap 来实现 LRU 算法
 * HashMap key 保存的是值，Value 保存的是 LRUNode
 */
public class LRUCache {
    private DTreeNode root;
    private DTreeNode tail;
    private Map<String, DTreeNode> cache;
    private int cap;

    public LRUCache(int cap) {
        root = new DTreeNode(null, null, null, null);
        tail = new DTreeNode(null, null, null, null);
        root.next = tail;
        tail.pre = root;
        cache = new HashMap<>(cap);
        this.cap = cap;
    }

    private void deleteNode(String item) {
        if (cache.containsKey(item)) {
            DTreeNode curNode = cache.get(item);
            DTreeNode pre = curNode.pre;
            DTreeNode next = curNode.next;
            next.pre = pre;
            pre.next = next;
            cache.remove(item);
        }
    }
    private void deleteTail() {
        DTreeNode pre = tail.pre;
        DTreeNode pre1 = pre.pre;
        pre1.next = tail;
        tail.pre = pre1;
        cache.remove(pre.key);
    }

    private void insertFirst(String item, Integer value) {
        DTreeNode curNode = new DTreeNode(item, value, root, null);
        DTreeNode rootNext = root.next;
        root.next = curNode;
        rootNext.pre = curNode;
        curNode.next = rootNext;
        cache.put(item, curNode);
    }

    public void put(String key, Integer value) {
        this.deleteNode(key);
        if (cache.size() == cap) {
            deleteTail();
        }
        this.insertFirst(key, value);
    }

    public Integer visit(String item) {
        if (cache.containsKey(item)) {
            DTreeNode node = cache.get(item);
            this.put(item, node.value);
            return node.value;
        } else {
            return -1;
        }
    }

    @Override
    public String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        DTreeNode cur = this.root.next;
        while (cur.value != null) {
            stringBuilder.append(cur.key).append(":").append(cur.value).append("|");
            cur = cur.next;
        }
        return stringBuilder.toString();
    }

    public static void main(String[] args) {
        LRUCache lruCache = new LRUCache(3);
        System.out.println(lruCache);
        lruCache.put("item1", 1);
        System.out.println(lruCache);
        lruCache.put("item2", 2);
        System.out.println(lruCache);
        lruCache.put("item2", 3);
        System.out.println(lruCache);
        lruCache.put("item4", 4);
        System.out.println(lruCache);
        lruCache.put("item5", 5);
        System.out.println(lruCache);
        lruCache.visit("item2");
        System.out.println(lruCache);
        lruCache.put("item1", 1);
        System.out.println(lruCache);
    }
}
