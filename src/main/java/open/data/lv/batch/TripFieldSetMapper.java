package open.data.lv.batch;

import open.data.lv.model.Validation;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;
import org.springframework.validation.BindException;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class TripFieldSetMapper implements FieldSetMapper<Validation> {

    //Ier_ID,Parks,TranspVeids,GarNr,MarsrNos,TMarsruts,Virziens,ValidTalonaId,Laiks
    @Override
    public Validation mapFieldSet(FieldSet fieldSet) throws BindException {
        String[] values = fieldSet.getValues();
        LocalDateTime arr = LocalTime
                .parse(fieldSet.getValues()[8], DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss"))
                .atDate(LocalDate.of( 2018, 5, 16));
        Date start = Date.from(arr.atZone(ZoneId.systemDefault()).toInstant());
        return new Validation(values[3], values[5].trim(), values[7].trim(), start);
    }

}
