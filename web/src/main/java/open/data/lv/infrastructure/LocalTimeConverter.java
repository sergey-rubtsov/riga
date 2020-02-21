package open.data.lv.infrastructure;

import org.springframework.core.convert.converter.Converter;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

public final class LocalTimeConverter implements Converter<String, LocalTime> {

    private static final DateTimeFormatter timeFormatter = DateTimeFormatter.ofPattern("HH:mm:ss");

    public LocalTimeConverter() {
    }

    @Override
    public LocalTime convert(String source) {
        if (source == null || source.isEmpty()) {
            return null;
        }
        return LocalTime.parse(source, timeFormatter);
    }
}
