package open.data.lv.repository;

import open.data.lv.model.Validation;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.stream.Stream;

@Repository
public interface TripRepository extends JpaRepository<Validation, Long> {

    @Query("select v from Validation v where v.timestamp between ?1 and ?2 and v.route = ?3")
    List<Validation> findValidationsByRoute(Date from, Date to, String route);

    Stream<Validation> findValidationsByTicketAndTimestampGreaterThan(String ticket, Date timestamp);

    @Query("select v from Validation v where where v.timestamp between ?1 and ?2 and v.ticket in " +
            "(select v.ticket from Validation v where v.timestamp between ?1 and ?2 " +
            "group by v.ticket having count(v.ticket) > 1) ORDER BY v.ticket")
    List<Validation> findValidationsByTicket(Date from, Date to);

    @Query("select v from Validation v where v.start between ?1 and ?2")
    Stream<Validation> findValidationsByTime(Date from, Date to);
}
