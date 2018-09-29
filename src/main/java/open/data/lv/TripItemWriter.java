package open.data.lv;

import open.data.lv.model.Person;
import open.data.lv.model.Transport;
import open.data.lv.model.Trip;
import open.data.lv.repository.PersonRepository;
import open.data.lv.repository.ScheduleRepository;
import open.data.lv.repository.TransportRepository;
import open.data.lv.repository.TripRepository;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

//@Component
public class TripItemWriter implements ItemWriter<Trip>, JobExecutionListener {

    @Autowired
    private TripRepository tripRepository;

    @Autowired
    private PersonRepository personRepository;

    @Autowired
    private TransportRepository transportRepository;

    @Override
    @Transactional
    public void write(List<? extends Trip> items) throws Exception {
        items.forEach(item -> {
            Person person = personRepository.findByTicket(item.getPerson().getTicket());
            person = Optional.ofNullable(person).orElse(item.getPerson());
            person = personRepository.save(person);
            person.getTrips().add(item);
            item.setPerson(person);
            Transport transport = transportRepository.findByUuid(item.getTransport().getUuid());
            transport = Optional.ofNullable(transport).orElse(item.getTransport());
            item.setTransport(transport);
            transportRepository.save(transport);
            tripRepository.save(item);
        });
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {

    }

    @Override
    public void afterJob(JobExecution jobExecution) {

    }
}
