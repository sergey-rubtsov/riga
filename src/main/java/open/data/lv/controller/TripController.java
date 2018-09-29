package open.data.lv.controller;

import open.data.lv.message.TripMessage;
import open.data.lv.service.TripService;
import open.data.lv.translator.TripTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping(value = "/api")
public class TripController {

    @Autowired
    private TripService tripService;

    @Autowired
    private TripTranslator tripTranslator;

    @RequestMapping("/trips")
    public List<TripMessage> getTrips(@RequestParam("time") String time,
                                      @RequestParam("route") String route) {
        return tripTranslator.translate(tripService.getTrips(time, route));
    }

    @RequestMapping("/test")
    public List<TripMessage> getTrips() {
        return tripTranslator.translate(tripService.getTrips("16.05.2018 10:00:00", "A 10"));
    }

    @RequestMapping("/job")
    public void process() {
        tripService.processTrips("16.05.2018 10:00:00");
    }

}
