package bigdata.clebeg.cn.base;

import com.sun.org.apache.regexp.internal.RE;

import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingDeque;

/**
 * 红黑树 是所有节点不是红色就是黑色的 自平衡 二叉查找树 红黑树的特点
 * 1. 所有节点不是黑色就是红色
 * 2. 根节点一定是黑色
 * 3. 叶子节点一定是黑色
 * 4. 任意红色节点的子节点一定都是黑色
 * 5. 从任意节点到所有叶子节点包含的黑色节点的数量一定是相同的
 * 正常红黑树只有create和查找操作，不涉及更新删除操作
 * create 操作是比较难的 查找操作就是普通的二分查找
 * https://www.cnblogs.com/skywang12345/p/3624343.html
 */

class RedBlackTreeNode {
    public boolean color;  // false: 黑色 true: 红色
    public RedBlackTreeNode parent; // 父亲节点
    public RedBlackTreeNode left; // 左子节点
    public RedBlackTreeNode right; // 右子节点
    public double value; // 当前存的值

    public RedBlackTreeNode() {
    }

    public RedBlackTreeNode(boolean color, RedBlackTreeNode parent, RedBlackTreeNode left, RedBlackTreeNode right, double value) {
        this.color = color;
        this.parent = parent;
        this.left = left;
        this.right = right;
        this.value = value;
    }

}

public class RedBlackTree {
    private final static boolean RED = true;
    private final static boolean BLACK = false;
    private RedBlackTreeNode root; // 根节点
    private Iterator<Double> valueIt; // 所有

    /**
     * 给红黑色插入新的值
     * @param value
     */
    private void insert(Double value) {

    }

    private void changeColor(RedBlackTreeNode curNode, boolean color) {
        curNode.color = color;
    }

    private void setBlack(RedBlackTreeNode curNode) {
        changeColor(curNode, BLACK);
    }

    private void setRed(RedBlackTreeNode curNode) {
        changeColor(curNode, RED);
    }

    private boolean isRed(RedBlackTreeNode curNode) {
        return curNode.color == RED;
    }

    private boolean isBlack(RedBlackTreeNode curNode) {
        return curNode.color == BLACK;
    }

    private RedBlackTreeNode parentOf(RedBlackTreeNode curNode) {
        if (curNode == null) {
            return null;
        } else {
            return curNode.parent;
        }
    }

    
    /**
     * 红黑树插入修正函数
     * 在向红黑树中插入节点之后(失去平衡)，再调用该函数
     * 目的是将它重新塑造成一颗红黑树
     * @param node 插入的结点
     */
    private void insertFixUp(RedBlackTreeNode node) {
        RedBlackTreeNode parent, grandParent;
        parent = parentOf(node);
        // 若父节点存在，并且父节点的颜色是红色 如果是黑色什么也不用做
        while (parent != null && isRed(parent)) {
            // 如果父节点是红色 那么祖父节点一定存在
            grandParent = parentOf(parent);

            // 若父节点是祖父节点的左孩子
            if (parent == grandParent.left) {
                // case 1：叔叔节点是红色
                RedBlackTreeNode uncle = grandParent.right;
                if (uncle != null && isRed(uncle)) {
                    // 递归继续
                    setBlack(uncle);
                    setBlack(parent);
                    setRed(grandParent);
                    node = grandParent;
                    parent = parentOf(node);
                    continue;
                }

                // Case 2：叔叔是黑色 或者 null，且当前节点是右孩子
                // 此时为3角关系 需要先按父亲节点逆时针旋转 得到3点一线 然后将当前节点设置为原来的 parent
                if (parent.right == node) {
                    RedBlackTreeNode tmp;
                    leftRotate(parent);
                    tmp = parent;
                    parent = node;
                    node = tmp;
                }

                // Case 3：叔叔是黑色，且当前节点是左孩子 此时3点一线
                setBlack(parent);
                setRed(grandParent);
                rightRotate(grandParent);
            } else {
                // 若父节点是祖父节点的右孩子 对称操作
                // Case 1条件：叔叔节点是红色
                RedBlackTreeNode uncle = grandParent.left;
                if ( uncle != null && isRed(uncle)) {
                    setBlack(uncle);
                    setBlack(parent);
                    setRed(grandParent);
                    node = grandParent;
                    continue;
                }

                // Case 2：叔叔是黑色，且当前节点是左孩子
                if (parent.left == node) {
                    RedBlackTreeNode tmp;
                    rightRotate(parent);
                    tmp = parent;
                    parent = node;
                    node = tmp;
                }

                // Case 3：叔叔是黑色，且当前节点是右孩子。
                setBlack(parent);
                setRed(grandParent);
                leftRotate(grandParent);
            }
        }

        // 将根节点设为黑色
        setBlack(this.root);
    }

    private void rightRotate(RedBlackTreeNode curNode) {
        RedBlackTreeNode parent = curNode.parent;
        // rightSon 一定不为空
        RedBlackTreeNode leftSon = curNode.left;
        RedBlackTreeNode leftSonRightSon = leftSon.right;

        if (parent == null) {
            this.root = leftSon;
        } else {
            if (parent.left == curNode) {
                parent.left = leftSon;
            } else {
                parent.right = leftSon;
            }
        }

        curNode.parent = leftSon;
        curNode.left = leftSonRightSon;

        leftSon.parent = parent;
        leftSon.right = curNode;

        if (leftSonRightSon != null) {
            leftSonRightSon.parent = curNode;
        }
    }

    /**
     * 当前节点左旋也是逆时针旋转，旋转点为当前节点的 rightSon，会变成自己的父亲节点
     * 有几个节点需要改变
     * 1. 当前节点父亲节点：非空儿子节点需要改 空右儿子变为 root
     * 2. 当前节点：父亲要改成原来右儿子 右儿子变为原来右儿子的左儿子
     * 3. 当前节点右儿子：父亲变为当前节点的父亲 左儿子变为当前节点
     * 4. 当前节点右儿子的左儿子：非空父亲改为当前节点，空什么不用做
     * @param curNode
     */
    private void leftRotate(RedBlackTreeNode curNode) {
        RedBlackTreeNode parent = curNode.parent;
        // rightSon 一定不为空
        RedBlackTreeNode rightSon = curNode.right;
        RedBlackTreeNode rightSonLeftSon = rightSon.left;

        if (parent == null) {
            this.root = rightSon;
        } else {
            if (parent.left == curNode) {
                parent.left = rightSon;
            } else {
                parent.right = rightSon;
            }
        }

        curNode.parent = rightSon;
        curNode.right = rightSonLeftSon;

        rightSon.parent = parent;
        rightSon.left = curNode;

        if (rightSonLeftSon != null) {
            rightSonLeftSon.parent = curNode;
        }

    }

    public RedBlackTree() {

    }

    public static RedBlackTree make(Collection<Double> valueList) {
        return new RedBlackTree();
    }
}
