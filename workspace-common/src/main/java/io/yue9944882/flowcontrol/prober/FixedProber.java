package io.yue9944882.flowcontrol.prober;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.Map;

import io.yue9944882.flowcontrol.param.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FixedProber implements Prober {

	private static final Logger log = LoggerFactory.getLogger(FixedProber.class);

	private final Map<Integer, OffsetDateTime> backoffs = new HashMap<>();
	private final Duration BACKOFF = Duration.ofMillis(Parameters.CONSUMER_FAILURE_BACKOFF_MILLIS);

	@Override
	public void recordFailure(int shard) {
		OffsetDateTime backoff = OffsetDateTime.now().plus(BACKOFF);
		backoffs.compute(shard, (k, v) -> {
			if (v == null) {
				return backoff;
			}
			if (v.isBefore(backoff)) {
				return backoff;
			}
			return v;
		});
		log.info("FAILURE: {}", shard);
	}

	@Override
	public boolean isHealthy(int shard) {
		if (!backoffs.containsKey(shard)) {
			return true;
		}
		return OffsetDateTime.now().isAfter(backoffs.get(shard));
	}

}
