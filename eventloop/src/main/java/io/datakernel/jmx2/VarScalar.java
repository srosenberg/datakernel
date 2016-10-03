package io.datakernel.jmx2;

public interface VarScalar extends Var {
	Object get();

	void set(Object value);

//	VarTypeScalar getType();
}
