package open.data.lv.infrastructure;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Optional;

public final class LocalDateTimeConverter implements Converter<String, LocalDateTime> {

    private static final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy HH:mm:ss");

    private static final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("dd.MM.yyyy");

    public LocalDateTimeConverter() {
    }

    @Override
    public LocalDateTime convert(String source) {
        try {
            return Optional.ofNullable(source)
                    .map(localDate -> LocalDateTime.parse(localDate, dateTimeFormatter)).orElse(null);
        } catch (DateTimeParseException dpe) {
            return Optional.ofNullable(source)
                    .map(localDate -> LocalDate.parse(localDate, dateFormatter).atStartOfDay()).orElse(null);
        }
    }
}
