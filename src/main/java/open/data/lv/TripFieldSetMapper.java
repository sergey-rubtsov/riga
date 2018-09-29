package open.data.lv;

import open.data.lv.model.Person;
import open.data.lv.model.Transport;
import open.data.lv.model.Trip;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TripFieldSetMapper implements FieldSetMapper<Trip> {

    //Ier_ID,Parks,TranspVeids,GarNr,MarsrNos,TMarsruts,Virziens,ValidTalonaId,Laiks
    @Override
    public Trip mapFieldSet(FieldSet fieldSet) throws BindException {
        String[] values = fieldSet.getValues();
        Transport transport = new Transport(values[3], values[5].trim());
        Person person = new Person(values[7].trim());
        LocalDateTime arr = LocalTime.parse(fieldSet.getValues()[8], DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss")).atDate(LocalDate.of( 2018, 5, 16));
        Date start = Date.from(arr.atZone(ZoneId.systemDefault()).toInstant());
        return new Trip(transport, person, start);
    }

}
