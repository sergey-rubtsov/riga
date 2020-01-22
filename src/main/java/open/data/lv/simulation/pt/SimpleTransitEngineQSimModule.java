package open.data.lv.simulation.pt;

import org.matsim.core.mobsim.qsim.AbstractQSimModule;
import org.matsim.core.mobsim.qsim.components.QSimComponentsConfig;
import org.matsim.core.mobsim.qsim.pt.TransitEngineModule;

/**
 * @author Sebastian Hörl / ETHZ
 */
public class SimpleTransitEngineQSimModule extends AbstractQSimModule {
	public static final String COMPONENT_NAME = "SimpleTransit";
	
	@Override
	protected void configureQSim() {
		bind(SimpleTransitQSimEngine.class).asEagerSingleton();
		addQSimComponentBinding(COMPONENT_NAME).to(SimpleTransitQSimEngine.class);
	}
	
	static public void configure(QSimComponentsConfig components) {
		if (components.hasNamedComponent(TransitEngineModule.TRANSIT_ENGINE_NAME)) {
			components.removeNamedComponent(TransitEngineModule.TRANSIT_ENGINE_NAME);
		}
		
		components.addNamedComponent(COMPONENT_NAME);
	}
}
