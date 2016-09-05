package io.datakernel.jmx2;

public interface VarScalar<T> extends Var {
	T get();
	void set(T value);
}
