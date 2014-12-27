package io.github.lukaszbudnik.avro.model;


import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class SerialiseDeserialiseTest {

    @Test
    public void shouldSerialiseDeserialiseTest() throws IOException {
        TestCaseV1 testCaseV1 = TestCaseV1.newBuilder()
                .setName("test case v1")
                .build();

        TestRunV1 testRunV1 = TestRunV1.newBuilder()
                .setName("test run v1")
                .setTestCases(Arrays.asList(testCaseV1))
                .build();

        DatumWriter<TestRunV1> writer = new SpecificDatumWriter<TestRunV1>(TestRunV1.getClassSchema());
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        Encoder out = EncoderFactory.get().jsonEncoder(TestRunV1.getClassSchema(), baos);
        writer.write(testRunV1, out);
        out.flush();
        String json = baos.toString();

        System.out.println(json);

        DatumReader<TestRunV2> reader = new SpecificDatumReader<TestRunV2>(TestRunV1.getClassSchema(), TestRunV2.getClassSchema());
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        Decoder in = DecoderFactory.get().jsonDecoder(TestRunV2.getClassSchema(), bais);
        TestRunV2 testRunV2 = reader.read(new TestRunV2(), in);

        Assert.assertEquals(testRunV1.getName(), testRunV2.getName());
        Assert.assertEquals(testRunV1.getTestCases(), testRunV2.getTestCases());
        Assert.assertNull(testRunV2.getTimezone());
    }

}
