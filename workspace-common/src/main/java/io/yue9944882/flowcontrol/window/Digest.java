package io.yue9944882.flowcontrol.window;

import java.util.Objects;

import org.apache.dubbo.rpc.Invocation;

public class Digest {

	public Digest(int seq, String input) {
		this.seq = seq;
		this.input = input;
	}

	private final int seq;
	private final String input;

	public int getSeq() {
		return seq;
	}

	public String getInput() {
		return input;
	}

	public static Digest parse(Invocation invocation, String keyName, String keyType) {
		String input = invocation.getAttachment(keyName);
		String pt = keyName.substring(keyType.length());
		int seq = Integer.parseInt(pt);
		return new Digest(seq, input);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		Digest digest = (Digest) o;
		return seq == digest.seq && Objects.equals(input, digest.input);
	}

	@Override
	public int hashCode() {
		return Objects.hash(seq, input);
	}
}
