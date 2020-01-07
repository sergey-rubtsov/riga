package open.data.lv.simulation;

import org.apache.log4j.Logger;
import org.matsim.api.core.v01.Scenario;
import org.matsim.api.core.v01.network.Network;
import org.matsim.core.api.experimental.events.EventsManager;
import org.matsim.core.config.Config;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.config.ConfigWriter;
import org.matsim.core.controler.PrepareForSimUtils;
import org.matsim.core.events.EventsUtils;
import org.matsim.core.gbl.MatsimRandom;
import org.matsim.core.mobsim.qsim.AgentTracker;
import org.matsim.core.mobsim.qsim.QSim;
import org.matsim.core.mobsim.qsim.QSimBuilder;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.pt.transitSchedule.api.TransitSchedule;
import org.matsim.vis.otfvis.OTFClientLive;
import org.matsim.vis.otfvis.OnTheFlyServer;
import org.matsim.vis.otfvis.handler.FacilityDrawer;
import org.matsim.vis.snapshotwriters.AgentSnapshotInfoFactory;
import org.matsim.vis.snapshotwriters.SnapshotLinkWidthCalculator;

import java.io.PrintWriter;
import java.io.StringWriter;

public class Simulation {

    private static final Logger log = Logger.getLogger(Simulation.class);

    public static void main(String[] args) {
        playConfig("real/simulation/config.xml");
    }

    public static void playConfig(final String url) {
        Config config = ConfigUtils.loadConfig(url);
        MatsimRandom.reset(config.global().getRandomSeed());
        log.info("Complete config dump:");
        StringWriter writer = new StringWriter();
        new ConfigWriter(config).writeStream(new PrintWriter(writer));
        log.info("\n\n" + writer.getBuffer().toString());
        log.info("Complete config dump done.");
        Scenario scenario = ScenarioUtils.loadScenario(config);
        playScenario(scenario);
    }

    public static void playScenario(Scenario scenario){
        EventsManager events = EventsUtils.createEventsManager();
        PrepareForSimUtils.createDefaultPrepareForSim(scenario).run();
        QSim qSim = new QSimBuilder(scenario.getConfig()) //
                .useDefaults() //
                .build(scenario, events);

        OnTheFlyServer server = startServerAndRegisterWithQSim(scenario.getConfig(),scenario, events, qSim);
        OTFClientLive.run(scenario.getConfig(), server);

        qSim.run();
    }

    public static OnTheFlyServer startServerAndRegisterWithQSim(Config config, Scenario scenario, EventsManager events, QSim qSim) {
        return startServerAndRegisterWithQSim(config, scenario, events, qSim, null);
    }

    public static OnTheFlyServer startServerAndRegisterWithQSim(Config config, Scenario scenario, EventsManager events, QSim qSim,
                                                                OnTheFlyServer.NonPlanAgentQueryHelper nonPlanAgentQueryHelper) {
        OnTheFlyServer server = OnTheFlyServer.createInstance(scenario, events, qSim, nonPlanAgentQueryHelper);

        if (config.transit().isUseTransit()) {

            Network network = scenario.getNetwork();
            TransitSchedule transitSchedule = scenario.getTransitSchedule();
            SnapshotLinkWidthCalculator linkWidthCalculator = new SnapshotLinkWidthCalculator();
            linkWidthCalculator.setLinkWidthForVis( config.qsim().getLinkWidthForVis() );
            if (! Double.isNaN(network.getEffectiveLaneWidth())){
                linkWidthCalculator.setLaneWidth( network.getEffectiveLaneWidth() );
            }
            AgentSnapshotInfoFactory snapshotInfoFactory = new AgentSnapshotInfoFactory(linkWidthCalculator);

            for ( AgentTracker agentTracker : qSim.getAgentTrackers() ) {
                FacilityDrawer.Writer facilityWriter = new FacilityDrawer.Writer(network, transitSchedule, agentTracker, snapshotInfoFactory);
                server.addAdditionalElement(facilityWriter);
            }
        }

        server.pause();
        return server;
    }
}
