package io.datakernel.jmx2;

import java.util.Map;

public interface VarMap extends Var {
	Map<Object, Var> getMap();
}
