package io.yue9944882.flowcontrol.traffic;

public class Constants {
	public static final String REQ_HEADER_KEY_SEQ = "seq";
	public static final String REQ_HEADER_KEY_SHARD = "shard";
	public static final String REQ_HEADER_KEY_MOD = "mod";
	public static final String REQ_HEADER_KEY_INDEX = "index";
	public static final String REQ_HEADER_KEY_FINISHED = "finished";
	public static final String REQ_HEADER_KEY_LEFT = "left";
	public static final String REQ_HEADER_KEY_RIGHT = "right";
	public static final String REQ_HEADER_KEY_PREFIX_PUSH = "push-";
	public static final String REQ_HEADER_KEY_PREFIX_RECALL = "recall-";

	public static final String RESP_HEADER_KEY_PREFIX_SEQ = "seq-";
	public static final String RESP_HEADER_KEY_AVG_COST_MILLIS = "avg";
}
