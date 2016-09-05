package io.datakernel.jmx2;

public interface VarTypeMap extends VarType {
	VarTypeScalar getKeyType();

	VarType getValueType();
}
