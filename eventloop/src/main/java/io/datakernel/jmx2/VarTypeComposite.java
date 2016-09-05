package io.datakernel.jmx2;

import java.util.Map;

public interface VarTypeComposite extends Var {
	Map<String, VarType> getFieldTypes();
}
