package open.data.lv;

import open.data.lv.model.Schedule;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class ScheduleFieldSetMapper implements FieldSetMapper<Schedule> {

    //,stop_id,stop_name,stop_lat,stop_lon,route_id,route_short_name,route_long_name,route_type,service_id,trip_id,shape_id,arrival_time,departure_time,stop_sequence
    //0,1518,Ferrum Agro,56.96953,24.37367,riga_bus_16,16,"Abrenes iela - Papīrfabrika ""Jugla""",3,11266,2713,riga_bus_16_b2-a,06:51:00,06:51:00,2
    @Override
    public Schedule mapFieldSet(FieldSet fieldSet) throws BindException {
        String value = fieldSet.getValues()[5];
        String[] split = value.split("_");
        if ("bus".equals(split[1])) {
            value = "A " + split[2];
        } else if ("tram".equals(split[1])) {
            value = "Tm " + split[2];
        } else if ("trol".equals(split[1])) {
           value = "Tr " + split[2];
        }
        //10:14:00
        //16.05.2018
        Date arrival;
        Date depart;
        try {
            LocalDateTime arr = LocalTime.parse(fieldSet.getValues()[12], DateTimeFormatter.ofPattern("HH:mm:ss")).atDate(LocalDate.of( 2018, 5, 16));
            arrival = Date.from(arr.atZone(ZoneId.systemDefault()).toInstant());
        } catch (DateTimeException ex) {
            arrival = Date.from(LocalDate.of( 2018, 5, 16).atTime(23,59).atZone(ZoneId.systemDefault()).toInstant());
        }
        try {
            LocalDateTime dep = LocalTime.parse(fieldSet.getValues()[13], DateTimeFormatter.ofPattern("HH:mm:ss")).atDate(LocalDate.of( 2018, 5, 16));
            depart = Date.from(dep.atZone(ZoneId.systemDefault()).toInstant());
        } catch (DateTimeException ex) {
            depart = Date.from(LocalDate.of( 2018, 5, 16).atTime(23,59).atZone(ZoneId.systemDefault()).toInstant());
        }
        return new Schedule(arrival, depart, fieldSet.readDouble(3), fieldSet.readDouble(4), value);
    }
}
