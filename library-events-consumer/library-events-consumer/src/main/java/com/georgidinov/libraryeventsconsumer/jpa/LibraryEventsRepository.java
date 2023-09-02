package com.georgidinov.libraryeventsconsumer.jpa;

import com.georgidinov.libraryeventsconsumer.entity.LibraryEvent;
import org.springframework.data.repository.CrudRepository;

public interface LibraryEventsRepository extends CrudRepository<LibraryEvent, Integer> {
}
