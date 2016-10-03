package io.datakernel.jmx2;

import java.util.Map;

public interface VarTypeComposite extends VarType {
	Map<String, VarType> getFieldTypes();
}
