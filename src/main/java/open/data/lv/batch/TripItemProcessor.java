package open.data.lv.batch;

import open.data.lv.model.Validation;
import org.springframework.batch.item.ItemProcessor;

public class TripItemProcessor implements ItemProcessor<Validation, Validation> {

    @Override
    public Validation process(Validation item) throws Exception {
        return item;
    }

}
