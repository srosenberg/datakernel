package io.datakernel.bytebufnew;

import java.nio.ByteBuffer;

public class ByteBufN {
	static final class ByteBufNSlice extends ByteBufN {
		private ByteBufN root;

		private ByteBufNSlice(ByteBufN buf, int rPos, int wPos, int limit) {
			super(buf.array, rPos, wPos, limit);
			this.root = buf;
		}

		@Override
		public void recycle() {
			root.recycle();
		}

		@Override
		public ByteBufN slice(int offset, int limit) {
			return root.slice(offset, limit);
		}

		@Override
		boolean isRecycled() {
			return root.isRecycled();
		}
	}

	protected final byte[] array;

	private int rPos;
	private int wPos;

	public final int limit;

	int refs;

	// creators
	private ByteBufN(byte[] array, int rPos, int wPos, int limit) {
		assert rPos >= 0 && wPos <= limit && rPos <= wPos && limit <= array.length;
		this.array = array;
		this.rPos = rPos;
		this.wPos = wPos;
		this.limit = limit;
	}

	public static ByteBufN empty() {
		return new ByteBufN(new byte[0], 0, 0, 0);
	}

	public static ByteBufN create(int size) {
		return new ByteBufN(new byte[size], 0, 0, size);
	}

	public static ByteBufN wrap(byte[] bytes) {
		return wrap(bytes, 0, bytes.length);
	}

	public static ByteBufN wrap(byte[] bytes, int offset, int length) {
		int limit = offset + length;
		return new ByteBufN(bytes, offset, limit, limit);
	}

	// slicing
	public ByteBufN slice() {
		return slice(rPos, wPos);
	}

	public ByteBufN slice(int size) {
		return slice(rPos, rPos + size);
	}

	public ByteBufN slice(int offset, int limit) {
		assert !isRecycled();
		if (!isRecycleNeeded()) {
			return ByteBufN.wrap(array, offset, limit - offset);
		}
		refs++;
		return new ByteBufNSlice(this, offset, limit, limit);
	}

	// recycling
	public void recycle() {
		assert !isRecycled();
		if (refs > 0 && --refs == 0) {
			assert --refs == -1;
			ByteBufNPool.recycle(this);
		}
	}

	boolean isRecycled() {
		return refs == -1;
	}

	public boolean isRecycleNeeded() {
		return refs > 0;
	}

	void reset() {
		assert isRecycled();
		refs = 1;
		rewind();
	}

	// byte buffers
	// assume ByteBuffer is being passed in 'read mode' pos=0; lim=wPos
	public ByteBuffer toByteBuffer() {
		assert !isRecycled();
		ByteBuffer buffer = ByteBuffer.wrap(array, rPos, limit - rPos);
		buffer.position(wPos);
		return buffer;
	}

	// assume ByteBuffer is being passed in 'read mode' pos=0, lim=wPos
	public void setByteBuffer(ByteBuffer buffer) {
		assert !isRecycled();
		assert this.array == buffer.array();
		assert buffer.arrayOffset() == 0;
		setReadPosition(buffer.position());
		setWritePosition(buffer.limit());
	}

	// getters
	public byte[] array() {
		return array;
	}

	public int remainingToWrite() {
		assert !isRecycled();
		return limit - wPos;
	}

	public int remainingToRead() {
		assert !isRecycled();
		return wPos - rPos;
	}

	public boolean canWrite() {
		assert !isRecycled();
		return wPos != limit;
	}

	public boolean canRead() {
		assert !isRecycled();
		return rPos != wPos;
	}

	public int getReadPosition() {
		assert !isRecycled();
		return rPos;
	}

	public int getWritePosition() {
		assert !isRecycled();
		return wPos;
	}

	public void skip(int size) {
		assert !isRecycled();
		assert rPos + size <= limit;
		rPos += size;
	}

	public byte get() {
		assert !isRecycled();
		assert rPos < wPos;
		return array[rPos++];
	}

	/**
	 * Get byte from underlying array
	 * @param index absolute position in underlying array
	 * @return byte at specified position in underlying array
	 */
	public byte at(int index) {
		assert !isRecycled();
		assert index <= limit;
		return array[index];
	}

	public byte peek() {
		assert !isRecycled();
		return array[rPos];
	}

	public byte peek(int offset) {
		assert !isRecycled();
		assert (rPos + offset) < wPos;
		return array[rPos + offset];
	}

	public void drainTo(byte[] array, int offset, int size) {
		assert !isRecycled();
		assert size >= 0 && (offset + size) <= array.length;
		assert this.rPos + size <= this.wPos;
		System.arraycopy(this.array, this.rPos, array, offset, size);
		this.rPos += size;
	}

	public void drainTo(ByteBufN buf, int size) {
		assert !buf.isRecycled();
		drainTo(buf.array, buf.wPos, size);
		buf.wPos += size;
	}

	// editing

	/**
	 * Set byte in underlying array
	 * @param index absolute position at underlying array
	 * @param b byte to be set
	 */
	public void set(int index, byte b) {
		assert !isRecycled();
		assert index >= rPos && index < limit && index >= wPos;
		array[index] = b;
	}

	public void put(byte b) {
		set(wPos, b);
		wPos++;
	}

	public void put(ByteBufN buf) {
		put(buf.array, buf.rPos, buf.wPos);
		buf.rPos = buf.wPos;
	}

	public void put(byte[] bytes) {
		put(bytes, 0, bytes.length);
	}

	public void put(byte[] bytes, int off, int lim) {
		assert !isRecycled();
		assert wPos + (lim - off) <= limit;
		assert bytes.length >= lim;
		int length = lim - off;
		System.arraycopy(bytes, off, array, wPos, length);
		wPos += length;
	}

	public void setReadPosition(int pos) {
		assert !isRecycled();
		assert pos >= rPos && pos <= wPos;
		this.rPos = pos;
	}

	public void setWritePosition(int pos) {
		assert !isRecycled();
		assert pos >= rPos && pos <= limit;
		this.wPos = pos;
	}

	public void rewind() {
		wPos = 0;
		rPos = 0;
	}

	// miscellaneous
	@Override
	public boolean equals(Object o) {
		assert !isRecycled();
		if (this == o) return true;
		if (o == null || !(ByteBufN.class == o.getClass() || ByteBufNSlice.class == o.getClass())) return false;

		ByteBufN buf = (ByteBufN) o;

		return remainingToRead() == buf.remainingToRead() &&
				arraysEquals(this.array, this.rPos, this.wPos, buf.array, buf.rPos);
	}

	private boolean arraysEquals(byte[] array, int offset, int limit, byte[] arr, int off) {
		for (int i = 0; i < limit - offset; i++) {
			if (array[offset + i] != arr[off + i]) {
				return false;
			}
		}
		return true;
	}

	@Override
	public int hashCode() {
		assert !isRecycled();
		int result = 1;
		for (int i = rPos; i < wPos; i++) {
			result = 31 * result + array[i];
		}
		return result;
	}

	@Override
	public String toString() {
		char[] chars = new char[remainingToRead() < 256 ? remainingToRead() : 256];
		for (int i = 0; i < chars.length; i++) {
			byte b = array[rPos + i];
			chars[i] = (b >= ' ') ? (char) b : (char) 65533;
		}
		return new String(chars);
	}
}