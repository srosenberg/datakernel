package io.datakernel.jmx2;

public interface Display {
	Display getFieldDisplay(String field);

	void exportScalar(VarTypeScalar type, VarScalar<?> var);

	void exportList(VarTypeList type, VarList var);

	void exportMap(VarTypeMap type, VarMap var);
}
