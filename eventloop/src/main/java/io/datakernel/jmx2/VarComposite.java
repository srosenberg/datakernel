package io.datakernel.jmx2;

public interface VarComposite extends Var {
	Var get(String field);
//	VarTypeComposite getType();
}
