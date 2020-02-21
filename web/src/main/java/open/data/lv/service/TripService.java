package open.data.lv.service;

import open.data.lv.model.Validation;
import open.data.lv.repository.TripRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

@Service
public class TripService {

    @Autowired
    private TripRepository tripRepository;

    public List<Validation> getTripsDuringHour(LocalDateTime begin, String route) {
        Date from = Date.from(begin.atZone(ZoneId.systemDefault()).toInstant());
        Date to = Date.from(begin.plusHours(1).atZone(ZoneId.systemDefault()).toInstant());
        return tripRepository.findValidationsByRoute(from, to, route);
    }

    @Transactional
    public void processTripsDuringHour(LocalDateTime begin) {
        Date from = Date.from(begin.atZone(ZoneId.systemDefault()).toInstant());
        Date to = Date.from(begin.plusHours(1).atZone(ZoneId.systemDefault()).toInstant());
        List<Validation> validations = tripRepository.findValidationsByTicket(from, to);



/*        Date from = Date.from(begin.atZone(ZoneId.systemDefault()).toInstant());
        Date to = Date.from(begin.plusHours(1).atZone(ZoneId.systemDefault()).toInstant());
        Stream<Validation> validations = tripRepository.findTripsByTime(from, to);
        validations.forEach(el -> {
            Stream<Validation> found = tripRepository
                    .findTripsByTicketAndStartGreaterThan(el.getTicket(), el.getTimestamp());
            found.forEach(eq -> {
                el.setEnd(eq.getEnd());
                el.setX1(eq.getX1());
                el.setY1(eq.getY1());
                tripRepository.save(el);
            });
        });*/
    }

}
