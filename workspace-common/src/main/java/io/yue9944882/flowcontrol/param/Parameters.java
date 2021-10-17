package io.yue9944882.flowcontrol.param;

public class Parameters {
	public static final int GATLIN_POOL_COUNT = 80;
	public static final int GATLIN_TRANSMIT_INTERVAL_MILLIS = 1;
	public static final int GATLIN_WINDOW_LOCKMOD = 1;

	public static final int CONSUMER_FAILURE_BACKOFF_MILLIS = 100;
	public static final int CONSUMER_SEM_RELEASE_TIMEOUT_MILLIS = 50;

	public static final double PROVIDER_PID_P = 20D;
	public static final double PROVIDER_PID_I = 0D;
	public static final double PROVIDER_PID_D = 10D;

	public static final int PROVIDER_PID_CONCURRENCY_MIN = 40;
	public static final int PROVIDER_PID_CONCURRENCY_MAX = 300;

	public static final int PROVIDER_PID_TRACK_RING_CAPACITY = 3000;
	public static final int PROVIDER_PID_TRACK_RING_EFFECTIVE_CAPACITY = 2000;

	public static final int PROVIDER_PID_CONTROL_DELAY_TARGET_MILLIS = 11600;
	public static final int PROVIDER_PID_CONTROL_DELAY_TARGET_THRESHOLD = 100;
	public static final int PROVIDER_PID_CONTROL_DELAY_INTERVAL_MILLIS = 20;

	public static final int PROVIDER_MAX_PROCESS_RECALL = 100;
	public static final int PROVIDER_MAX_DEQUEUE_RECALL = 200;

	public static final long PROVIDER_PROCESS_TOTAL_TIMEOUT_MILLIS = 3;
	public static final long PROVIDER_PROCESS_RECALL_TIMEOUT_MILLIS = 2;

	public static final int PROVIDER_LRU_CACHE_CAPACITY = 5000;

	public static final int PROVIDER_QUEUE_CHUNK = 600;
	public static final int PROVIDER_QUEUE_OFFSET = 100;
}
