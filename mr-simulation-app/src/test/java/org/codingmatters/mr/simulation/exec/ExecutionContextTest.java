package org.codingmatters.mr.simulation.exec;

import jdk.nashorn.api.scripting.NashornScriptEngineFactory;
import org.hamcrest.Matchers;
import org.junit.Test;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import java.time.temporal.ChronoField;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;

public class ExecutionContextTest {

    @Test
    public void givenProperlyFormattedIsoDateTime__whenGettingMonth__thenMonthOfYearIsReturned() throws Exception {
        assertThat(ExecutionContext.dateField(ChronoField.MONTH_OF_YEAR, "2019-02-17T09:36:55Z"), is(2));
    }

    @Test
    public void givenNotIsoDateTimeFormatted__whenGettingMonth__thenMinusOneIsReturned() throws Exception {
        assertThat(ExecutionContext.dateField(ChronoField.MONTH_OF_YEAR, "17/02/2019"), is(-1));
    }

    @Test
    public void given__when__then() throws Exception {
        ScriptEngine engine = EngineProvider.createEngine();
        Bindings bindings = engine.createBindings();

//        ExecutionContext.setup(engine);
        ExecutionContext.setup(bindings);

        assertThat(engine.eval("dateYear('2019-02-17T09:36:55Z')", bindings), is(2019));
        assertThat(engine.eval("dateMonth('2019-02-17T09:36:55Z')", bindings), is(2));
        assertThat(engine.eval("dateDay('2019-02-17T09:36:55Z')", bindings), is(17));
        assertThat(engine.eval("dateHour('2019-02-17T09:36:55Z')", bindings), is(9));
        assertThat(engine.eval("dateMinute('2019-02-17T09:36:55Z')", bindings), is(36));
        assertThat(engine.eval("dateSecond('2019-02-17T09:36:55Z')", bindings), is(55));
    }
}