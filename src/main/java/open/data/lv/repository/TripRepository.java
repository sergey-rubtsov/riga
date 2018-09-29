package open.data.lv.repository;

import open.data.lv.model.Trip;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.stream.Stream;


//SELECT Street, City, COUNT(*)
// FROM yourtable
// GROUP BY Street, City
// HAVING COUNT(*) > 1
@Repository
public interface TripRepository extends JpaRepository<Trip, Long> {

    @Query("select tr from Trip tr where tr.start between ?1 and ?2 and tr.route = ?3")
    List<Trip> findTripsByRoute(Date from, Date to, String route);

    Stream<Trip> findTripsByTicketAndStartGreaterThan(String ticket, Date start);
//"SELECT e.salary, COUNT(e) FROM Employee e GROUP BY e.salary"
//    @Query("select tr from Trip tr join (select tr.ticket from Trip tr group by tr.ticket having count(tr.ticket) > 1) B" +
//            " on (tr.ticket = B.ticket)")

    @Query("select tr from Trip tr where tr.ticket in (select tr.ticket from Trip tr group by tr.ticket having count(tr.ticket) > 1) ORDER BY tr.ticket")
    //@Query("select tr.ticket from Trip tr group by tr.ticket having count(tr.ticket) > 1")
    List<Trip> findTripsByTicket();

    @Query("select tr from Trip tr where tr.start between ?1 and ?2")
    Stream<Trip> findTripsByTime(Date from, Date to);
}
