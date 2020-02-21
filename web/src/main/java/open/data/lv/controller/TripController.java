package open.data.lv.controller;

import open.data.lv.message.TripMessage;
import open.data.lv.service.TripService;
import open.data.lv.translator.TripTranslator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

@RestController
@RequestMapping(value = "/api")
public class TripController {

    @Autowired
    private TripService tripService;

    @Autowired
    private TripTranslator tripTranslator;

    @RequestMapping("/trips")
    public List<TripMessage> getTrips(@RequestParam("time") LocalDateTime begin,
                                      @RequestParam("route") String route) {
        //return tripTranslator.translate(tripService.getTripsDuringHour(begin, route));
        return null;
    }

    @RequestMapping("/job")
    public void process(@RequestParam("time") LocalDateTime begin) {
        tripService.processTripsDuringHour(begin);
    }

/*    @RequestMapping("/test")
    public List<TripMessage> getTrips() {
        return tripTranslator.translate(tripService.getTripsDuringHour("16.05.2018 10:00:00", "A 10"));
    }
*/

}
