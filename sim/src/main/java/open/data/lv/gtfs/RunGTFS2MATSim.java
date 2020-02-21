package open.data.lv.gtfs;

import java.time.LocalDate;

import org.matsim.api.core.v01.Scenario;
import org.matsim.core.config.ConfigUtils;
import org.matsim.core.scenario.ScenarioUtils;
import org.matsim.core.utils.geometry.CoordinateTransformation;
import org.matsim.core.utils.geometry.transformations.IdentityTransformation;
import org.matsim.pt.transitSchedule.api.TransitScheduleWriter;

import com.conveyal.gtfs.GTFSFeed;
import com.conveyal.gtfs.model.Route;

/**
 * @author NKuehnel
 */
public class RunGTFS2MATSim {

    /**
     * Starts the conversion.
     * 
     * @param fromFile path of input file
     * @param toFile path to write to
     * @param date date to check for transit data
     * @param transformation coordination transformation for stops
	 * @param useExtendedRouteTypes transfer extended route types to MATSim schedule
     */
    public static void convertGtfs(String fromFile, String toFile, LocalDate date, CoordinateTransformation transformation, boolean useExtendedRouteTypes) {
		GTFSFeed feed = GTFSFeed.fromFile(fromFile);

		feed.feedInfo.values().stream().findFirst().ifPresent(feedInfo -> {
			System.out.println("Feed start date: " + feedInfo.feed_start_date);
			System.out.println("Feed end date: " + feedInfo.feed_end_date);
		});

		System.out.println("Parsed trips: "+feed.trips.size());
		System.out.println("Parsed routes: "+feed.routes.size());
		System.out.println("Parsed stops: "+feed.stops.size());

		Scenario scenario = ScenarioUtils.createScenario(ConfigUtils.createConfig());

		GtfsConverter converter = new GtfsConverter(feed, scenario, transformation, false);
		converter.setDate(date);
		converter.convert();

		System.out.println("Converted stops: " + scenario.getTransitSchedule().getFacilities().size());

		TransitScheduleWriter writer = new TransitScheduleWriter(scenario.getTransitSchedule());
		writer.writeFile(toFile);

		System.out.println("Done.");
    }

	public static void main(String[] args) {
		String inputZipFile = args[0];
		String outputFile = args[1];
		String date = args[2];
		boolean useExtendedRouteTypes = Boolean.parseBoolean(args[3]);
		convertGtfs(inputZipFile, outputFile, LocalDate.parse(date), new IdentityTransformation(), useExtendedRouteTypes);
	}

}
