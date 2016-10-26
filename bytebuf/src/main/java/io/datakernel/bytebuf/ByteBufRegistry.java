/*
 * Copyright (C) 2015 SoftIndex LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.datakernel.bytebuf;

import java.lang.ref.Reference;
import java.lang.ref.SoftReference;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class ByteBufRegistry {
	private static final ConcurrentHashMap<ByteBufWrapper, ByteBufMetaInfo> activeByteBufs = new ConcurrentHashMap<>();

	private static volatile boolean storeStackTrace = false;

	// TODO(vmykhalko): maybe add some scheduled task (using timer?) for cleaning up empty soft references

	private ByteBufRegistry() {}

	// region public api
	public static void recordAllocate(ByteBuf buf) {
		assert recordAllocateIfAssertionsEnabled(buf);
	}

	public static void recordRecycle(ByteBuf buf) {
		assert recordRecycleIfAssertionsEnabled(buf);
	}

	public static Map<ByteBufWrapper, ByteBufMetaInfo> getActiveByteBufs() {
		return activeByteBufs;
	}

	public static void setStoreStackTrace(boolean store) {
		storeStackTrace = store;
	}

	public static boolean getStoreStackTrace() {
		return storeStackTrace;
	}
	// endregion

	private static boolean recordAllocateIfAssertionsEnabled(ByteBuf buf) {
		StackTraceElement[] stackTrace = null;
		if (storeStackTrace) {
			// TODO(vmykhalko): maybe use new Exception().getStackTrace instead ? according to performance issues
			StackTraceElement[] fullStackTrace = Thread.currentThread().getStackTrace();
			// remove stack trace lines that stand for registration method calls
			stackTrace = Arrays.copyOfRange(fullStackTrace, 3, fullStackTrace.length);
		}
		long timestamp = System.currentTimeMillis();
		ByteBufMetaInfo metaInfo = new ByteBufMetaInfo(stackTrace, timestamp);
		activeByteBufs.put(new ByteBufWrapper(buf), metaInfo);

		return true;
	}

	private static boolean recordRecycleIfAssertionsEnabled(ByteBuf buf) {
		activeByteBufs.remove(new ByteBufWrapper(buf));

		return true;
	}

	private static void removeEmptyWrappers() {
		Iterator<Map.Entry<ByteBufWrapper, ByteBufMetaInfo>> iterator = activeByteBufs.entrySet().iterator();
		while (iterator.hasNext()) {
			ByteBufWrapper wrapper = iterator.next().getKey();
			if (wrapper.getByteBuf() == null) {
				iterator.remove();
			}
		}
	}

	static final class ByteBufWrapper {
		private final Reference<ByteBuf> bufRef;

		public ByteBufWrapper(ByteBuf buf) {
			this.bufRef = new SoftReference<>(buf);
		}

		public ByteBuf getByteBuf() {
			return bufRef.get();
		}

		// TODO(vmykhalko): check again equals and hashcode implementations for boundary conditions
		@Override
		public boolean equals(Object o) {
			if (o == null) {
				return false;
			}

			if (o.getClass() != ByteBufWrapper.class) {
				return false;
			}

			ByteBufWrapper other = (ByteBufWrapper) o;
			return this.bufRef.get() == other.bufRef.get();
		}

		@Override
		public int hashCode() {
			return System.identityHashCode(bufRef.get());
		}
	}

	static final class ByteBufMetaInfo {
		private final StackTraceElement[] stackTrace;
		private final long allocateTimestamp;

		public ByteBufMetaInfo(StackTraceElement[] stackTrace, long allocateTimestamp) {
			this.stackTrace = stackTrace;
			this.allocateTimestamp = allocateTimestamp;
		}

		public StackTraceElement[] getStackTrace() {
			return stackTrace;
		}

		public long getAllocationTimestamp() {
			return allocateTimestamp;
		}
	}
}
