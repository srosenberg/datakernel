package io.datakernel.bytebufnew;

import io.datakernel.jmx.MBeanFormat;
import io.datakernel.util.ConcurrentStack;

import javax.management.ObjectName;
import java.util.ArrayList;
import java.util.List;

import static io.datakernel.util.Preconditions.check;
import static java.lang.Integer.numberOfLeadingZeros;

public class ByteBufNPool {
	private static final int NUMBER_SLABS = 33;

	private static int minSize = 32;
	private static int maxSize = 1 << 30;

	private static final ConcurrentStack<ByteBufN>[] slabs = createSlabs(NUMBER_SLABS);
	private static final int[] created = new int[NUMBER_SLABS];

	// allocating
	public static ByteBufN allocateAtLeast(int size) {
		if (size < minSize || size >= maxSize) {
			return ByteBufN.create(size); // не хотим регистрировать в пуле
		}
		int index = 32 - numberOfLeadingZeros(size - 1); // index==32 for size==0
		ConcurrentStack<ByteBufN> queue = slabs[index];
		ByteBufN buf = queue.pop();
		if (buf != null) {
			buf.reset();
		} else {
			buf = ByteBufN.create(1 << index);
			buf.refs++;
			created[index]++;
		}
		return buf;
	}

	public static ByteBufN reallocateAtLeast(ByteBufN buf, int newSize) {
		assert !(buf instanceof ByteBufN.ByteBufNSlice);
		if (buf.limit >= newSize && (buf.limit <= minSize || numberOfLeadingZeros(buf.limit - 1) == numberOfLeadingZeros(newSize - 1))) {
			return buf;
		}
		ByteBufN newBuf = allocateAtLeast(newSize);
		newBuf.put(buf);
		buf.recycle();
		return newBuf;
	}

	public static ByteBufN concat(ByteBufN buf1, ByteBufN buf2) {
		assert !buf1.isRecycled() && !buf2.isRecycled();
		if (buf1.remainingToWrite() < buf2.remainingToRead()) {
			ByteBufN newBuf = allocateAtLeast(buf1.getWritePosition() + buf2.remainingToRead());
			newBuf.put(buf1);
			buf1.recycle();
			buf1 = newBuf;
		}
		buf1.put(buf2);
		buf2.recycle();
		return buf1;
	}

	public static void recycle(ByteBufN buf) {
		assert buf.array.length >= minSize && buf.array.length <= maxSize;
		int index = 32 - numberOfLeadingZeros(buf.array.length - 1);
		ConcurrentStack<ByteBufN> queue = slabs[index];
		assert !queue.contains(buf) : "duplicate recycle array";
		queue.push(buf);
	}

	public static ConcurrentStack<ByteBufN>[] getPool() {
		return slabs;
	}

	public static void clear() {
		for (int i = 0; i < ByteBufNPool.NUMBER_SLABS; i++) {
			slabs[i].clear();
			created[i] = 0;
		}
	}

	/*inner*/
	private static ConcurrentStack<ByteBufN>[] createSlabs(int numberOfSlabs) {
		//noinspection unchecked
		ConcurrentStack<ByteBufN>[] slabs = new ConcurrentStack[numberOfSlabs];
		for (int i = 0; i < slabs.length; i++) {
			slabs[i] = new ConcurrentStack<>();
		}
		return slabs;
	}

	//region  +jmx
	public static final ObjectName JMX_NAME = MBeanFormat.name(ByteBufNPool.class.getPackage().getName(), ByteBufNPool.class.getSimpleName());

	private static final ByteBufNPool.ByteBufNPoolStats stats = new ByteBufNPool.ByteBufNPoolStats();

	public static ByteBufNPool.ByteBufNPoolStats getStats() {
		return stats;
	}

	public static int getCreatedItems() {
		int items = 0;
		for (int n : created) {
			items += n;
		}
		return items;
	}

	public static int getCreatedItems(int slab) {
		check(slab >= 0 && slab < slabs.length);
		return created[slab];
	}

	public static int getPoolItems(int slab) {
		check(slab >= 0 && slab < slabs.length);
		return slabs[slab].size();
	}

	public static int getPoolItems() {
		int result = 0;
		for (ConcurrentStack<ByteBufN> slab : slabs) {
			result += slab.size();
		}
		return result;
	}

	public static String getPoolItemsString() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < ByteBufNPool.NUMBER_SLABS; ++i) {
			int createdItems = ByteBufNPool.getCreatedItems(i);
			int poolItems = ByteBufNPool.getPoolItems(i);
			if (createdItems != poolItems) {
				sb.append(String.format("Slab %d (%d) ", i, (1 << i)))
						.append(" created: ").append(createdItems)
						.append(" pool: ").append(poolItems).append("\n");
			}
		}
		return sb.toString();
	}

	private static long getPoolSize() {
		assert slabs.length == 33 : "Except slabs[32] that contains ByteBufs with size 0";
		long result = 0;
		for (int i = 0; i < slabs.length - 1; i++) {
			long slotSize = 1L << i;
			result += slotSize * slabs[i].size();
		}
		return result;
	}

	public static void setSizes(int minSize, int maxSize) {
		ByteBufNPool.minSize = minSize;
		ByteBufNPool.maxSize = maxSize;
	}

	public interface ByteBufPoolStatsMXBean {

		int getCreatedItems();

		int getPoolItems();

		long getPoolItemAvgSize();

		long getPoolSizeKB();

		List<String> getPoolSlabs();
	}

	public static final class ByteBufNPoolStats implements ByteBufNPool.ByteBufPoolStatsMXBean {

		@Override
		public int getCreatedItems() {
			return ByteBufNPool.getCreatedItems();
		}

		@Override
		public int getPoolItems() {
			return ByteBufNPool.getPoolItems();
		}

		@Override
		public long getPoolItemAvgSize() {
			int result = 0;
			for (ConcurrentStack<ByteBufN> slab : slabs) {
				result += slab.size();
			}
			int items = result;
			return items == 0 ? 0 : ByteBufNPool.getPoolSize() / items;
		}

		@Override
		public long getPoolSizeKB() {
			return ByteBufNPool.getPoolSize() / 1024;
		}

		@Override
		public List<String> getPoolSlabs() {
			assert slabs.length == 33 : "Except slabs[32] that contains ByteBufs with size 0";
			List<String> result = new ArrayList<>(slabs.length + 1);
			result.add("SlotSize,Created,InPool,Total(Kb)");
			for (int i = 0; i < slabs.length; i++) {
				long slotSize = 1L << i;
				int count = slabs[i].size();
				result.add((slotSize & 0xffffffffL) + "," + created[i] + "," + count + "," + slotSize * count / 1024);
			}
			return result;
		}
	}
	//endregion
}
