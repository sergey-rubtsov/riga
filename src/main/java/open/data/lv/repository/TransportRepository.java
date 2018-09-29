package open.data.lv.repository;

import open.data.lv.model.Transport;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TransportRepository  extends JpaRepository<Transport, Long> {

    Transport findByUuid(String uuid);

}
