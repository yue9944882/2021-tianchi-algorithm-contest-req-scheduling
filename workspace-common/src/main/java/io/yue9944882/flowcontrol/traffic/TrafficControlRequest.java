package io.yue9944882.flowcontrol.traffic;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

import io.yue9944882.flowcontrol.window.Digest;
import org.apache.dubbo.rpc.Invocation;

import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_INDEX;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_LEFT;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_MOD;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_PREFIX_PUSH;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_PREFIX_RECALL;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_RIGHT;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_SEQ;
import static io.yue9944882.flowcontrol.traffic.Constants.REQ_HEADER_KEY_SHARD;

public class TrafficControlRequest {

	public TrafficControlRequest(int seq, int shard, int mod, Invocation invocation) {
		this.seq = seq;
		this.invocation = invocation;
		this.recalls = new TreeMap<>(Integer::compareTo);
		this.pushes = new TreeMap<>(Integer::compareTo);
		this.invocation.setAttachment(REQ_HEADER_KEY_SEQ, seq);
		this.invocation.setObjectAttachment(REQ_HEADER_KEY_SHARD, shard);
		this.invocation.setObjectAttachment(REQ_HEADER_KEY_MOD, mod);
		this.shard = shard;
		this.mod = mod;
	}

	private final int seq;
	private final Invocation invocation;
	private int[] index;
	private final TreeMap<Integer, Digest> recalls;
	private final TreeMap<Integer, Digest> pushes;
	private final int shard;
	private final int mod;
	private int left;
	private int right;

	public int[] getIndex() {
		return index;
	}

	public int getSeq() {
		return seq;
	}

	public int getShard() {
		return shard;
	}

	public int getMod() {
		return mod;
	}

	public int getLeft() {
		return left;
	}

	public int getRight() {
		return right;
	}

	public TreeMap<Integer, Digest> getRecalls() {
		return recalls;
	}

	public TreeMap<Integer, Digest> getPushes() {
		return pushes;
	}

	public void appendPush(Digest digest) {
		invocation.setAttachment(REQ_HEADER_KEY_PREFIX_PUSH + digest.getSeq(), digest.getInput());
		this.pushes.put(digest.getSeq(), new Digest(digest.getSeq(), digest.getInput()));
	}

	public void appendRecall(Digest digest) {
		invocation.setAttachment(REQ_HEADER_KEY_PREFIX_RECALL + digest.getSeq(), digest.getInput());
		this.recalls.put(digest.getSeq(), new Digest(digest.getSeq(), digest.getInput()));
	}

	public void appendIndex(int[] seqs) {
		invocation.setAttachment(REQ_HEADER_KEY_INDEX, seqs);
		this.index = seqs;
	}

	public void setWindow(int left, int right) {
		invocation.setObjectAttachment(REQ_HEADER_KEY_LEFT, left);
		invocation.setObjectAttachment(REQ_HEADER_KEY_RIGHT, right);
	}

	public List<Digest> getOrderedPushes() {
		return buildOrdered(this.pushes);
	}

	public List<Digest> getOrderedRecalls() {
		return buildOrdered(this.recalls);
	}

	public Digest getRecall(int seq) {
		return this.recalls.get(seq);
	}

	public static int[] getIndex(Invocation invocation) {
		Object attachment = invocation.getObjectAttachment(REQ_HEADER_KEY_INDEX);
		if (attachment == null) {
			return new int[] {};
		}
		return (int[]) attachment;
	}

	public static int getLeft(Invocation invocation) {
		int left = -1;
		if (invocation.getObjectAttachment(REQ_HEADER_KEY_LEFT) != null) {
			left = (int) invocation.getObjectAttachment(REQ_HEADER_KEY_LEFT);
		}
		return left;
	}

	public static int getRight(Invocation invocation) {
		int right = -1;
		if (invocation.getObjectAttachment(REQ_HEADER_KEY_RIGHT) != null) {
			right = (int) invocation.getObjectAttachment(REQ_HEADER_KEY_RIGHT);
		}
		return right;
	}

	public static int getShard(Invocation invocation) {
		int shard = 0;
		if (invocation.getObjectAttachment(REQ_HEADER_KEY_SHARD) != null) {
			shard = (int) invocation.getObjectAttachment(REQ_HEADER_KEY_SHARD);
		}
		return shard;
	}

	public static int getMod(Invocation invocation) {
		int mod = 0;
		if (invocation.getObjectAttachment(REQ_HEADER_KEY_MOD) != null) {
			mod = (int) invocation.getObjectAttachment(REQ_HEADER_KEY_MOD);
		}
		return mod;
	}

	public static Digest getRecall(Invocation invocation, int seq) {
		return Digest.parse(
			invocation,
			REQ_HEADER_KEY_PREFIX_RECALL + seq,
			REQ_HEADER_KEY_PREFIX_RECALL);
	}

	private static List<Digest> buildOrdered(TreeMap<Integer, Digest> map) {
		TreeMap<Integer, Digest> copied = new TreeMap<>(map);
		List<Digest> ds = new ArrayList<>();
		while (copied.size() > 0) {
			Digest digest = copied.pollFirstEntry().getValue();
			ds.add(digest);
		}
		return ds;
	}

	public static int getSeq(Invocation invocation) {
		Object seq = invocation.getObjectAttachment(REQ_HEADER_KEY_SEQ);
		if (seq == null) {
			String seqStr = invocation.getAttachment(REQ_HEADER_KEY_SEQ);
			if (seqStr == null) {
				return -1;
			}
			return Integer.parseInt(seqStr);
		}
		return (Integer) seq;
	}

}
