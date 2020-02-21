/*
 * Copyright (C) Schweizerische Bundesbahnen SBB, 2018.
 */

package open.data.lv.pt;

import org.matsim.core.config.ReflectiveConfigGroup;
import org.matsim.core.utils.collections.CollectionUtils;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author mrieser / SBB
 */
public class SimpleTransitConfigGroup extends ReflectiveConfigGroup {

    static public final String GROUP_NAME = "SimplePt";

    static private final String PARAM_DETERMINISTIC_SERVICE_MODES = "deterministicServiceModes";
    static private final String PARAM_CREATE_LINK_EVENTS_INTERVAL = "createLinkEventsInterval";

    private Set<String> deterministicServiceModes = new HashSet<>();
    private int createLinkEventsInterval = 0;

    public SimpleTransitConfigGroup() {
        super(GROUP_NAME);
    }

    @StringGetter(PARAM_DETERMINISTIC_SERVICE_MODES)
    private String getDeterministicServiceModesAsString() {
        return CollectionUtils.setToString(this.deterministicServiceModes);
    }

    public Set<String> getDeterministicServiceModes() {
        return this.deterministicServiceModes;
    }

    @StringSetter(PARAM_DETERMINISTIC_SERVICE_MODES)
    private void setDeterministicServiceModes(String modes) {
        setDeterministicServiceModes(CollectionUtils.stringToSet(modes));
    }

    public void setDeterministicServiceModes(Set<String> modes) {
        this.deterministicServiceModes.clear();
        this.deterministicServiceModes.addAll(modes);
    }

    @StringGetter(PARAM_CREATE_LINK_EVENTS_INTERVAL)
    public int getCreateLinkEventsInterval() {
        return this.createLinkEventsInterval;
    }

    @StringSetter(PARAM_CREATE_LINK_EVENTS_INTERVAL)
    public void setCreateLinkEventsInterval(int value) {
        this.createLinkEventsInterval = value;
    }

    @Override
    public Map<String, String> getComments() {
        Map<String, String> comments = super.getComments();
        comments.put(PARAM_DETERMINISTIC_SERVICE_MODES, "Leg modes used by the created transit drivers that should be simulated strictly according to the schedule.");
        comments.put(PARAM_CREATE_LINK_EVENTS_INTERVAL, "(iterationNumber % createLinkEventsInterval) == 0 defines in which iterations linkEnter- and linkLeave-events are created,\n" +
                "\t\t\t\t\"useful for visualization or analysis purposes. Defaults to 0. `0' disables the creation of events completely.");
        return comments;
    }
}
