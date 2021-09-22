package io.yue9944882.flowcontrol.traffic;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.yue9944882.flowcontrol.traffic.Constants.RESP_HEADER_KEY_PREFIX_SEQ;

public class TrafficControlResult {

	private static final Logger log = LoggerFactory.getLogger(TrafficControlResult.class);

	public TrafficControlResult(Result result) {
		this.result = result;
	}

	private final Result result;

	public void append(int seq, String input, Result child) {
		try {
			Integer resp = (Integer) child.recreate();
			result.setObjectAttachment(RESP_HEADER_KEY_PREFIX_SEQ + seq + "-" + input, resp);
		}
		catch (Throwable throwable) {
			log.error("", throwable);
		}
	}

	public static Map<Integer, Integer> parse(Result result) {
		Map<Integer, Integer> ret = new HashMap<>();
		for (Map.Entry<String, Object> entry : result.getObjectAttachments().entrySet()) {
			if (entry.getKey().startsWith(RESP_HEADER_KEY_PREFIX_SEQ)) {
				String pt = entry.getKey().substring(RESP_HEADER_KEY_PREFIX_SEQ.length());
				String seqStr = pt.substring(0, pt.indexOf('-'));
				int seq = Integer.parseInt(seqStr);
				ret.put(seq, (Integer) entry.getValue());
			}
		}
		return ret;
	}

	public static CraftResult[] parseResponses(Result result) {
		List<Result> results = new ArrayList<>();
		for (Map.Entry<Integer, Integer> entry : TrafficControlResult.parse(result).entrySet()) {
			CraftResult craft = new CraftResult(entry.getKey());
			craft.setValue(entry.getValue());
			craft.setAttachments(result.getAttachments());
			craft.setObjectAttachments(result.getObjectAttachments());
			results.add(craft);
		}
		return results.toArray(new CraftResult[0]);
	}

	public static class CraftResult extends AppResponse {
		public CraftResult(int seq) {
			this.seq = seq;
		}

		final int seq;

		public int getSeq() {
			return seq;
		}
	}

}
