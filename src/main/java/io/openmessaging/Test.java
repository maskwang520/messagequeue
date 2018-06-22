package io.openmessaging;

/**
 * Created by maskwang on 18-6-20.
 */
public class Test {

    public static void main(String[] args) {
        ListNode p1 = new ListNode(2);
        ListNode p2 = new ListNode(2);
        ListNode p3 = new ListNode(2);
        ListNode p4 = new ListNode(3);
        ListNode p5 = new ListNode(4);
        p1.next = p2;
        p2.next = p3;
        p3.next = p4;
        p4.next = p5;
        p5.next = null;
        deleteDuplicates(p1);


    }


    public static ListNode deleteDuplicates(ListNode head) {

        if (head==null||head.next == null) {
            return head;
        }

        ListNode pre = head, q = head.next, p = head;
        while (q != null) {
            if (p.val == q.val) {
                while (q != null && p.val == q.val) {
                    p = p.next;
                    q = q.next;
                }
                pre.next = q;
            }else{
                pre = p;
            }
            //pre = p;
            p = p.next;
            if(p!=null) {
                q = q.next;
            }

        }

        return head;
    }

    public static class ListNode {
        int val;
        ListNode next;

        ListNode(int x) {
            val = x;
            next = null;
        }
    }
}
