package io.github.rawwise.common;

import io.github.rawwise.common.event.Event;

public interface Service {
    void serve(Event<?> event);
}
