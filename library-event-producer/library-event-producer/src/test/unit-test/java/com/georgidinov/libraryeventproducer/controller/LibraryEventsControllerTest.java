package com.georgidinov.libraryeventproducer.controller;

import com.georgidinov.libraryeventproducer.domain.LibraryEvent;
import com.georgidinov.libraryeventproducer.producer.LibraryEventsProducer;
import com.georgidinov.libraryeventproducer.util.TestUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.BDDMockito;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.hamcrest.Matchers.equalTo;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class LibraryEventsControllerTest {

    @Mock
    LibraryEventsProducer eventsProducer;

    @InjectMocks
    LibraryEventsController controller;

    @InjectMocks
    LibraryEventControllerAdvise controllerAdvise;

    MockMvc mockMvc;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        mockMvc = MockMvcBuilders.standaloneSetup(controller).setControllerAdvice(controllerAdvise).build();
    }

    @Test
    void postLibraryEvent() throws Exception {
        //given
        LibraryEvent libraryEvent = TestUtil.libraryEventRecord();
        String eventJson = TestUtil.getObjectAsString(libraryEvent);
        //when
        BDDMockito
                .when(eventsProducer.sendLibraryEventApproachThree(libraryEvent))
                .thenReturn(null);
        //then
        mockMvc.perform(post("/v1/library-event").contentType(MediaType.APPLICATION_JSON).content(eventJson))
                .andExpect(status().isCreated());
    }

    @Test
    void postLibraryEvent_4XX() throws Exception {
        //given
        LibraryEvent libraryEvent = TestUtil.libraryEventRecordWithInvalidBook();
        String eventJson = TestUtil.getObjectAsString(libraryEvent);
        String expectedErrorMessage = "book.bookId - must not be null, book.bookName - must not be blank";
        //when
        BDDMockito
                .when(eventsProducer.sendLibraryEventApproachThree(libraryEvent))
                .thenReturn(null);
        //then
        mockMvc.perform(post("/v1/library-event").contentType(MediaType.APPLICATION_JSON).content(eventJson))
                .andExpect(status().is4xxClientError())
                .andExpect(content().contentType(MediaType.APPLICATION_JSON))
                .andExpect(jsonPath("$.error_message", equalTo(expectedErrorMessage)));
    }

}